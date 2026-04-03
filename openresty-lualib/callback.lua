-- callback.lua
-- Handles /telco/callback requests, logs to PostgreSQL, and sends SMS notifications

local cjson = require "cjson"
local pgmoon = require "pgmoon"
local sms    = require "ddm.mtsms"

package.path = package.path .. ";/usr/local/share/lua/5.2/?.lua;/usr/local/share/lua/5.2/?/init.lua"
package.cpath = package.cpath .. ";/usr/local/lib/lua/5.2/?.so"
package.path = package.path .. ";/usr/local/openresty/lualib/?.lua;/usr/local/openresty/lualib/?/init.lua"

local function respond(status, message)
    ngx.status = status
    ngx.header.content_type = "application/json"
    ngx.say(cjson.encode({ status = tostring(status), message = message }))
    return ngx.exit(status)
end

-- =====================================================================
-- Environment-based DB configuration (no hardcoded secrets)
-- =====================================================================
local function getenv(name, default)
  local v = os.getenv(name)
  if v == nil or v == '' then 
    if default ~= nil then return default end
    error(name .. " environment variable not set")
  end
  return v
end

local pg_host     = getenv("CALLBACK_DB_HOST", "127.0.0.1")
local pg_port     = getenv("CALLBACK_DB_PORT", "5432")
local pg_dbname   = getenv("CALLBACK_DB_NAME")
local pg_user     = getenv("CALLBACK_DB_USER")
local pg_password = getenv("CALLBACK_DB_PASSWORD")

ngx.req.read_body()
local body_data = ngx.req.get_body_data()
local ok, data = pcall(cjson.decode, body_data)
if not ok then
    return respond(400, "Invalid JSON payload")
end

local pg = pgmoon.new({
    host = pg_host,
    port = pg_port,
    database = pg_dbname,
    user = pg_user,
    password = pg_password
})

local ok, err = pg:connect()
if not ok then
    ngx.log(ngx.ERR, "PostgreSQL connection failed: ", err)
    return respond(500, "DB connection failed")
end

local function safe_escape(pg, val)
    if val == nil then return "NULL" end
    return pg:escape_literal(tostring(val))
end

local payload_json = cjson.encode(data)

-- Insert into ddm_callbacks with ON CONFLICT on actual unique columns
local sql_callbacks = string.format([[
    INSERT INTO ddm_callbacks (
        msisdn, caller_msisdn, transaction_id, operation_id,
        result, result_code, processing_time, payload, received_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (transaction_id,
                 COALESCE(msisdn, payload_msisdn::varchar),
                 COALESCE(operation_id, payload_operation::varchar),
                 COALESCE(payload_sequence, ''),
                 COALESCE(payload_request, ''))
    DO NOTHING;
]],
    safe_escape(pg, data.callingParty),
    safe_escape(pg, data.callerMsisdn),
    safe_escape(pg, data.sequenceNo or data.transactionId),
    safe_escape(pg, data.operationId),
    safe_escape(pg, data.result),
    safe_escape(pg, data.resultCode),
    safe_escape(pg, data.processingTime),
    pg:escape_literal(payload_json) .. "::jsonb"
)

local res, err = pg:query(sql_callbacks)
if not res then
    ngx.log(ngx.ERR, "Insert into ddm_callbacks failed: ", err)
    return respond(500, "Insert failed")
end

-- Map status
local function map_status(operationId, result, resultCode)
    local active_ops = { SN=true, PN=true, SR=true, RR=true, YR=true, GR=true }
    local inactive_ops = {
        ACI=true, SCI=true, GCI=true, SAC=true,
        RD=true, YD=true, GD=true, PD=true,
        PCI=true, BCI=true, PCE=true, ACE=true,
        GCE=true, SCE=true, BCE=true
    }
    if active_ops[operationId] then
        return "ACTIVE"
    elseif inactive_ops[operationId] then
        return "INACTIVE"
    elseif tostring(result):lower() == "failed" or tostring(resultCode) ~= "0" then
        return "FAILED"
    else
        return "UNKNOWN"
    end
end

local new_status = map_status(data.operationId, data.result, data.resultCode)

-- Determine msisdn
local msisdn_value = tostring(data.callingParty)
local caller_msisdn_value = tostring(data.callingParty)
local txn_id = tostring(data.sequenceNo or data.transactionId or "")

if txn_id ~= "" then
    local sql_ref = string.format(
        "SELECT referred_msisdn FROM ddm_referrals WHERE transaction_id=%s LIMIT 1",
        pg:escape_literal(txn_id)
    )
    local ref_res = pg:query(sql_ref)
    if ref_res and #ref_res > 0 and ref_res[1].referred_msisdn then
        msisdn_value = ref_res[1].referred_msisdn
    end
end

-- Upsert into ddm_subscribers
local sql_subscribers = string.format([[
    INSERT INTO ddm_subscribers (msisdn, caller_msisdn, transaction_id, status, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (msisdn)
    DO UPDATE SET status = EXCLUDED.status,
                  caller_msisdn = EXCLUDED.caller_msisdn,
                  transaction_id = EXCLUDED.transaction_id,
                  updated_at = NOW();
]],
    pg:escape_literal(msisdn_value),
    pg:escape_literal(caller_msisdn_value),
    pg:escape_literal(txn_id),
    pg:escape_literal(new_status)
)

local res2, err2 = pg:query(sql_subscribers)
if not res2 then
    ngx.log(ngx.ERR, "Upsert into ddm_subscribers failed: ", err2)
    return respond(500, "Subscriber update failed")
end

-- Referral confirmation
if txn_id ~= "" and new_status == "ACTIVE" then
    local sql_ref_update = string.format([[
        UPDATE ddm_referrals
        SET confirmed_at = NOW()
        WHERE transaction_id = %s;
    ]], pg:escape_literal(txn_id))
    pg:query(sql_ref_update)
end

-- SMS notifications
if new_status == "ACTIVE" then
    sms.send({ msisdn = msisdn_value, message = sms.msg_subscription_self(), messageId = "SUB-" .. txn_id })
    if msisdn_value ~= caller_msisdn_value then
        sms.send({ msisdn = caller_msisdn_value, message = sms.msg_subscription_sponsor(msisdn_value), messageId = "SPONSOR-" .. txn_id })
    end
elseif new_status == "INACTIVE" then
    sms.send({ msisdn = msisdn_value, message = sms.msg_unsubscription_self(), messageId = "UNSUB-" .. txn_id })
    if msisdn_value ~= caller_msisdn_value then
        sms.send({ msisdn = caller_msisdn_value, message = sms.msg_unsubscription_sponsor(msisdn_value), messageId = "UNSUB-SP-" .. txn_id })
    end
end

pg:keepalive()
return respond(200, "Callback processed successfully, status=" .. new_status)

