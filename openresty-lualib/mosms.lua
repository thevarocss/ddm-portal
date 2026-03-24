-- mosms.lua
-- Handles /telco/mosms requests (MO SMS) under OpenResty

local cjson  = require "cjson"
local pgmoon = require "pgmoon"
local sms    = require "ddm.mtsms"   -- SMS module

-- Add LuaRocks paths so OpenResty can see luaossl/pgmoon
package.path = package.path .. ";/usr/local/share/lua/5.2/?.lua;/usr/local/share/lua/5.2/?/init.lua"
package.cpath = package.cpath .. ";/usr/local/lib/lua/5.2/?.so"
package.path = package.path .. ";/usr/local/openresty/lualib/?.lua;/usr/local/openresty/lualib/?/init.lua"

-- State to track if we've already ACKed
local ack_sent = false

-- Utility: respond and exit
local function respond(status, message)
    ngx.status = status
    ngx.header.content_type = "application/json"
    ngx.say(cjson.encode({ status = tostring(status), message = message }))
    return ngx.exit(status)
end

-- Utility: early ACK without exiting (so we can continue processing)
local function ack_early()
    if ack_sent then return end
    ack_sent = true
    ngx.status = 200
    ngx.header.content_type = "application/json"
    ngx.say(cjson.encode({ status = "200", message = "MO-SMS processed successfully" }))
    -- Flush the response to Telco as early as possible
    ngx.flush(true)
end

-- Utility: write to dedicated log file
local function file_log(msg)
    local f, err = io.open("/usr/local/openresty/logs/mosms.log", "a")
    if f then
        f:write(os.date("%Y-%m-%d %H:%M:%S"), " ", msg, "\n")
        f:close()
    else
        ngx.log(ngx.ERR, "Failed to open mosms.log: ", err)
    end
end

-- CONFIG
local spId      = "thevardsub"
local password  = "7vW4w8s6Y1rk"
local serviceId = "234102200008429"
local telco_url = "http://41.76.198.1:6211/app/json/vas/initiateSubscription"

-- Product definitions
local PRODUCTS = {
  DMD = { plan="N100",  amount=100,  productId="23410220000051114" },
  DMW = { plan="N500",  amount=500,  productId="23410220000051115" },
  DMM = { plan="N1000", amount=1000, productId="23410220000051116" },
  DMQ = { plan="N2500", amount=2500, productId="23401220000031258" },
  DMY = { plan="N7500", amount=7500, productId="23401220000031259" }
}

-- Normalize MSISDN (only for 3rd-party local format)
local function normalize_msisdn(raw)
    if not raw then return nil end
    raw = tostring(raw):gsub("%s+", ""):gsub("[^%d]", "")
    if raw == "" then return nil end
    if raw:sub(1,1) == '+' then raw = raw:sub(2) end
    if raw:match("^234%d+$") and #raw == 13 then return raw end
    if raw:match("^0%d%d%d%d%d%d%d%d%d%d$") then return "234" .. raw:sub(2) end
    if raw:match("^%d%d%d%d%d%d%d%d%d%d$") then return "234" .. raw end
    return nil
end

-- Telco API call
local function telco_subscribe(msisdn, productId, amount)
    local timeStamp  = os.date("!%Y%m%d%H%M%S")
    local spPassword = ngx.md5(spId .. password .. timeStamp)
    local txn        = string.sub(ngx.md5(msisdn .. timeStamp .. tostring(math.random(1000,9999))), 1, 24)

    local payload_table = {
        msisdn        = msisdn,
        serviceId     = serviceId,
        productId     = productId,
        amount        = amount,
        channelId     = 2, -- SMS
        transactionId = txn
    }
    local payload = cjson.encode(payload_table)

    local cmd = string.format(
        "curl -s -X POST '%s' -H 'Content-Type: application/json' -H 'spId: %s' -H 'spPassword: %s' -H 'timeStamp: %s' -d '%s'",
        telco_url, spId, spPassword, timeStamp, payload:gsub("'", "'\\''")
    )

    file_log("Executing Telco API: " .. cmd)
    local fh = io.popen(cmd)
    local out = fh:read("*a") or ""
    fh:close()
    file_log("Telco raw response: " .. out)

    local ok, dec = pcall(cjson.decode, out)
    return (ok and dec) or {}, txn
end

-- Parse request body
ngx.req.read_body()
local body_data = ngx.req.get_body_data()
local ok, data = pcall(cjson.decode, body_data)
if not ok then
    file_log("Invalid JSON payload: " .. tostring(body_data))
    return respond(400, "Invalid JSON payload")
end

-- Immediately ACK receipt of MO SMS (per requirement)
ack_early()

-- Parse keyword and optional target
local parts = {}
for word in (data.message or ""):gmatch("%S+") do table.insert(parts, word) end
local keyword = parts[1] and parts[1]:upper()
local target  = parts[2]
local prod    = PRODUCTS[keyword]
if not prod then
    file_log("Invalid keyword: " .. tostring(keyword))

    -- Send invalid keyword SMS back to caller
    local caller_msisdn = tostring(data.senderAddress or "")
    if caller_msisdn:match("^234%d+$") and #caller_msisdn == 13 then
        local ok_invalid, resp_invalid = sms.send({
            msisdn    = caller_msisdn,
            message   = sms.msg_invalid_keyword(),
            messageId = "INVALID-" .. tostring(math.random(100000,999999))
        })
        if not ok_invalid then
            file_log("SMS(invalid keyword) failed: " .. cjson.encode(resp_invalid))
        else
            file_log("Invalid keyword SMS sent to " .. caller_msisdn)
        end
    else
        file_log("Invalid senderAddress for invalid keyword SMS: " .. caller_msisdn)
    end

    -- Already ACKed; stop further processing
    return
end

-- Caller always international format
local caller_msisdn = tostring(data.senderAddress or "")
if not caller_msisdn:match("^234%d+$") or #caller_msisdn ~= 13 then
    file_log("Invalid senderAddress: " .. caller_msisdn)
    return
end

-- Handle 3rd-party number
local referred_msisdn
if target then
    if target:match("^234%d+$") and #target == 13 then
        referred_msisdn = target
    else
        referred_msisdn = normalize_msisdn(target)
    end
else
    referred_msisdn = caller_msisdn
end
if not referred_msisdn then
    file_log("Invalid referred MSISDN: " .. tostring(target))
    return
end

file_log("MO-SMS received: caller=" .. caller_msisdn .. " keyword=" .. keyword .. " referred=" .. referred_msisdn)

-- Send subscription request to Telco (after ACK)
local resp, txn = telco_subscribe(referred_msisdn, prod.productId, prod.amount)
local status_code = tonumber(tostring(resp.status or resp.code or "0"))
file_log("Telco response status=" .. tostring(status_code) .. " txn=" .. txn)

if status_code == 200 then
    -- Consent SMS immediately on successful request
    local ok_consent, resp_consent = sms.send({
        msisdn    = referred_msisdn,
        message   = sms.msg_consent_request(),
        messageId = "CONSENT-" .. txn
    })
    if not ok_consent then
        file_log("SMS(consent subscriber) failed: " .. cjson.encode(resp_consent))
    end

    if referred_msisdn ~= caller_msisdn then
        sms.send({
            msisdn    = caller_msisdn,
            message   = "Consent required for " .. referred_msisdn ..
                        ": please ensure they dial *480*3#.",
            messageId = "CONSENT-SP-" .. txn
        })
    end
else
    file_log("Telco subscription failed: status=" .. tostring(status_code) .. " msg=" .. tostring(resp.message or cjson.encode(resp)))
end

-- Connect to Postgres for recording referral/subscriber
local pg = pgmoon.new({
    host = "127.0.0.1",
    port = 5432,
    database = "fusionpbx",
    user = "fusionpbx",
    password = "DXHoVfKQGjS3EKUk8eGrfzTZ4"
})
local ok_db, err_db = pg:connect()
if not ok_db then
    file_log("Postgres connection failed: " .. tostring(err_db))
    return
end

-- Save referral if 3rd-party
if referred_msisdn ~= caller_msisdn then
    local sql_ref = string.format([[
        INSERT INTO ddm_referrals (referrer_msisdn, referred_msisdn, product_id, service_id, transaction_id, channel)
        VALUES (%s, %s, %s, %s, %s, 'SMS')
        ON CONFLICT (referrer_msisdn, referred_msisdn) DO UPDATE
          SET product_id     = EXCLUDED.product_id,
              service_id     = EXCLUDED.service_id,
              transaction_id = EXCLUDED.transaction_id,
              channel        = EXCLUDED.channel,
              updated_at     = NOW();
    ]],
        pg:escape_literal(caller_msisdn),
        pg:escape_literal(referred_msisdn),
        pg:escape_literal(prod.productId),
        pg:escape_literal(serviceId),
        pg:escape_literal(txn)
    )
    local res_ref, err_ref = pg:query(sql_ref)
    if not res_ref then
        file_log("Referral upsert failed: " .. tostring(err_ref))
    else
        file_log("Referral saved: " .. caller_msisdn .. " -> " .. referred_msisdn .. " txn=" .. txn)
    end
end

-- Save subscriber with PENDING status
local sql_sub = string.format([[
    INSERT INTO ddm_subscribers (msisdn, caller_msisdn, plan, product_id, service_id, transaction_id, status, channel, last_access)
    VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', 'SMS', NOW())
    ON CONFLICT (msisdn) DO UPDATE
      SET caller_msisdn  = EXCLUDED.caller_msisdn,
          plan           = EXCLUDED.plan,
          product_id     = EXCLUDED.product_id,
          service_id     = EXCLUDED.service_id,
          transaction_id = EXCLUDED.transaction_id,
          status         = EXCLUDED.status,
          channel        = EXCLUDED.channel,
          last_access    = NOW();
]],
    pg:escape_literal(referred_msisdn),
    pg:escape_literal(caller_msisdn),
    pg:escape_literal(prod.plan),
    pg:escape_literal(prod.productId),
    pg:escape_literal(serviceId),
    pg:escape_literal(txn)
)
local res_sub, err_sub = pg:query(sql_sub)
if not res_sub then
    file_log("Subscriber upsert failed: " .. tostring(err_sub))
else
    file_log("Subscriber saved: " .. referred_msisdn .. " status=PENDING txn=" .. txn)
end

pg:keepalive()

-- Since we already ACKed early, do not send another response here.
-- End of processing.
