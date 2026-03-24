-- mtsms.lua
-- MT-SMS client module for sending notification and promotional SMS
-- Works in both OpenResty (ngx) and FreeSWITCH Lua (no ngx)

local cjson = require "cjson"

local M = {}

-- CONFIG: fill these with your SMS account/service settings
local SMS_URL        = "http://41.76.198.1:6211/app/json/sms/sendSmsRequest"
local SP_ID          = "thevardsms"
local SP_SECRET      = "7vW4w8s6Y1rk"   -- password used in MD5(spId + password + timeStamp)
local SERVICE_CODE   = "3003"           -- your assigned service code
local DEFAULT_NETWORK= "MTN"            -- or set to any valid value required by API

-- Environment detection
local has_ngx = (type(ngx) == "table")

-- Logger that works in both environments
local function log_info(...)
  local msg = table.concat({...}, " ")
  if has_ngx and ngx.log then
    ngx.log(ngx.INFO, msg)
  else
    io.stderr:write("[INFO] mtsms.lua: ", msg, "\n")
  end
end

local function log_err(...)
  local msg = table.concat({...}, " ")
  if has_ngx and ngx.log then
    ngx.log(ngx.ERR, msg)
  else
    io.stderr:write("[ERR] mtsms.lua: ", msg, "\n")
  end
end

-- MD5 helper: ngx.md5 -> md5 Lua rock -> shell md5sum
local function md5_hex(s)
  if has_ngx and ngx.md5 then
    return ngx.md5(s)
  end
  local ok, md5 = pcall(require, "md5")
  if ok and md5 and md5.sumhexa then
    return md5.sumhexa(s)
  end
  -- Fallback to shell md5sum
  local safe = tostring(s):gsub("'", "'\\''")
  local cmd = "printf %s '" .. safe .. "' | md5sum | awk '{print $1}'"
  local fh = io.popen(cmd)
  local out = fh and fh:read("*a") or ""
  if fh then fh:close() end
  local hex = out:match("%x+")
  if not hex or #hex == 0 then
    log_err("md5_hex fallback failed; input len=", #s, " raw_out=", out)
    return ""
  end
  return hex
end

local function make_headers()
  local ts = os.date("!%Y%m%d%H%M%S") -- UTC
  local spPassword = md5_hex(SP_ID .. SP_SECRET .. ts)
  return ts, spPassword
end

-- Curl executor (portable)
local function exec_curl(url, headers, body_json)
  local esc_body = body_json:gsub("'", "'\\''")
  local cmd = string.format(
    "curl -s -X POST '%s' -H 'Content-Type: application/json' -H 'spId: %s' -H 'spPassword: %s' -H 'timeStamp: %s' -d '%s'",
    url, headers.spId, headers.spPassword, headers.timeStamp, esc_body
  )
  log_info("MT-SMS curl: ", cmd)
  local fh = io.popen(cmd)
  local out = fh and fh:read("*a") or ""
  if fh then fh:close() end
  log_info("MT-SMS response: ", out)
  local ok, dec = pcall(cjson.decode, out)
  if ok then return dec end
  return { status = "0", message = "decode_error", raw = out }
end

-- Core sender
function M.send(opts)
  -- opts: { msisdn, message, keyword?, messageId, network?, serviceCode? }
  if not opts or not opts.msisdn or not opts.message or not opts.messageId then
    return false, "missing required fields"
  end
  local network     = opts.network     or DEFAULT_NETWORK
  local serviceCode = opts.serviceCode or SERVICE_CODE

  local timeStamp, spPassword = make_headers()
  if not spPassword or #spPassword == 0 then
    log_err("spPassword empty—MD5 failed; aborting send")
    return false, { status = "0", message = "md5_failed" }
  end

  local headers = { spId = SP_ID, spPassword = spPassword, timeStamp = timeStamp }

  local payload = {
    network     = network,
    msisdn      = opts.msisdn,
    serviceCode = serviceCode,
    message     = opts.message,
    keyword     = opts.keyword or nil,
    messageId   = opts.messageId
  }

  local resp = exec_curl(SMS_URL, headers, cjson.encode(payload))
  local code = tonumber(tostring(resp.status or "0"))
  local ok = (code == 200)
  if not ok then
    log_err("MT-SMS send failed: code=", tostring(code), " msg=", tostring(resp.message))
  end
  return ok, resp
end

-- Templated messages

-- Successful subscription (self)
function M.msg_subscription_self()
  return "You have been successfully subscribed to Diabetes Diet Master, " ..
         "you can call 3003 at anytime within your active subscription " ..
         "to listen to educative content on how to manage diabetes."
end

-- Successful subscription (sponsor notification)
function M.msg_subscription_sponsor(referred_msisdn)
  return referred_msisdn .. " has been successfully subscribed to Diabetes Diet Master, " ..
         "please contact them to dial 3003 to listen to educative content on how to manage diabetes."
end

-- Unsubscription (self)
function M.msg_unsubscription_self()
  return "You have been unsubscribed from Diabetes Diet Master. " ..
         "You can rejoin anytime to continue receiving guidance and content."
end

-- Unsubscription (sponsor notification)
function M.msg_unsubscription_sponsor(referred_msisdn)
  return referred_msisdn .. " has been unsubscribed from Diabetes Diet Master."
end

-- Consent request (Telco 2FA)
function M.msg_consent_request()
  return "Please dial *480*3# to confirm your subscription to Diabetes Diet Master."
end

-- Invalid SMS keyword received (optional)
function M.msg_invalid_keyword()
  return "You have sent an invalid keyword, please check and try again."
end

-- Promotional message helper
function M.msg_promo(text)
  return text
end

return M

