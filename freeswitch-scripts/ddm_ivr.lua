-- =====================================================================
-- ddm_ivr.lua
-- Diabetes Diet Master IVR (PostgreSQL, Telco MD5 auth, Self & 3rd-party)
-- Responsive menus: playAndGetDigits for immediate DTMF capture during playback
-- =====================================================================

-- environment
package.path = package.path .. ";/usr/share/lua/5.2/?.lua;/usr/share/lua/5.1/?.lua;/usr/share/lua/5.2/?/init.lua;/usr/share/lua/5.1/?/init.lua"
package.cpath = package.cpath .. ";/usr/lib/x86_64-linux-gnu/lua/5.2/?.so;/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"
package.path = package.path .. ";/usr/local/openresty/lualib/?.lua;/usr/local/openresty/lualib/?/init.lua"

local ok, json = pcall(require, "dkjson")
if not ok then ok, json = pcall(require, "cjson"); if not ok then json = nil end end
local ok2, md5_mod = pcall(require, "md5"); if not ok2 then md5_mod = nil end
local sms = require "ddm.mtsms"   -- import the SMS module

-- ===========================
-- CONFIG (Environment-driven, no hardcoded secrets)
-- ===========================
local function getenv(name, default)
  local v = os.getenv(name)
  if v == nil or v == '' then 
    if default ~= nil then return default end
    error(name .. " environment variable not set")
  end
  return v
end

-- SMS/Telco credentials (required)
local spId      = getenv("DDM_SMS_SP_ID")
local password  = getenv("DDM_SMS_SP_SECRET")
local serviceId = getenv("DDM_TELCO_SERVICE_ID")
local telco_url = getenv("DDM_TELCO_URL")

-- Database credentials (required)
local pg_host     = getenv("DDM_PG_HOST", "127.0.0.1")
local pg_port     = getenv("DDM_PG_PORT", "5432")
local pg_dbname   = getenv("DDM_PG_DB")
local pg_user     = getenv("DDM_PG_USER")
local pg_password = getenv("DDM_PG_PASSWORD")
local pg_options  = getenv("DDM_PG_OPTIONS", "-c client_encoding=UTF8")
local pg_conn_str = getenv("DDM_PG_CONN", string.format("pgsql://hostaddr=%s port=%s dbname=%s user=%s password=%s options='%s'", pg_host, pg_port, pg_dbname, pg_user, pg_password, pg_options))

-- Logging and paths
local LOG_ENABLED = getenv("DDM_LOG_ENABLED", "true") ~= "false"
local LOG_FILE = getenv("DDM_LOG_FILE", "/var/log/freeswitch/ddm_ivr.log")
local BASE = getenv("DDM_AUDIO_BASE", "/var/lib/freeswitch/recordings/62.164.214.102/")

-- =====================================================================
-- Product definitions (restored)
-- =====================================================================
local PRODUCTS = {
  [1] = { plan = "N100",  amount = 100,  productId = "23410220000051114" }, -- N100
  [2] = { plan = "N500",  amount = 500,  productId = "23410220000051115" }, -- N500
  [3] = { plan = "N1000", amount = 1000, productId = "23410220000051116" }, -- N1000
  [4] = { plan = "N2500", amount = 2500, productId = "23401220000031258" }, -- N2500
  [5] = { plan = "N7500", amount = 7500, productId = "23401220000031259" }  -- N7500
}

-- audio files registry
local FILES = {
  welcome            = BASE .. "Welcome.mp3",
  subscription_menu  = BASE .. "Subscription_menu.mp3",
  subscription_bgm   = BASE .. "Subscription_Bgm.mp3",
  failed_subscription= BASE .. "Failed_Subscription.mp3",
  network_failed = BASE .. "Network_Failure_Subscription_Failed.mp3",
  doi = {
    [100]  = BASE .. "DOI_N100.mp3",
    [500]  = BASE .. "DOI_N500.mp3",
    [1000] = BASE .. "DOI_N1000.mp3",
    [2500] = BASE .. "DOI_N2500.mp3",
    [7500] = BASE .. "DOI_N7500.mp3"
  },
  success_self       = BASE .. "Successful_Subscription_Self.mp3",
  success_3rd        = BASE .. "Successful_Subscription_3rd_Party.mp3",
  enter_3rd          = BASE .. "Enter_3rd_party.mp3",
  you_have_entered   = BASE .. "You_have_entered.mp3",
  confirm_prompt     = BASE .. "to_continue_or_to_enter_number_again.mp3",
  thank_you_referral = BASE .. "thank_you_referral.mp3",
  language_selection = BASE .. "Language_selection.mp3",
  you_have_chosen = {
    english = BASE .. "You_have_chosen_English.mp3",
    hausa   = BASE .. "You_have_chosen_Hausa.mp3",
    yoruba  = BASE .. "You_have_chosen_Yoruba.mp3",
    igbo    = BASE .. "You_have_chosen_Igbo.mp3",
    tiv     = BASE .. "You_have_chosen_Tiv.mp3",
    pidgin  = BASE .. "You_have_chosen_Pidgin.mp3"
  },
  failed_tech        = BASE .. "Subscription_failed_-_technical.mp3",
  failed_credit      = BASE .. "Subscription_failed_-_insufficient_credit.mp3",
  please_wait        = BASE .. "Please_wait_subscription_in_progress.mp3",
  cancelled          = BASE .. "Subscription_cancelled.mp3",
  invalid            = BASE .. "Invalid_Selection.mp3",
  system_error       = BASE .. "system_error.mp3"
}

-- language mapping & files (generic layout)
local LANGS = {
  english = {
    section = {[1] = BASE .. "English_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "English_Section_2_lessons_1_to_5.mp3"},
    lessons = {
      [1] = BASE .. "English_Lesson_1.mp3", [2] = BASE .. "English_Lesson_2.mp3", [3] = BASE .. "English_Lesson_3.mp3",
      [4] = BASE .. "English_Lesson_4.mp3", [5] = BASE .. "English_Lesson_5.mp3", [6] = BASE .. "English_Lesson_6.mp3",
      [7] = BASE .. "English_Lesson_7.mp3", [8] = BASE .. "English_Lesson_8.mp3", [9] = BASE .. "English_Lesson_9.mp3",
      [10] = BASE .. "English_Lesson_10.mp3"
    }
  },
  hausa = {
    section = {[1] = BASE .. "Hausa_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "Hausa_Section_2_lessons_6_to_10.mp3"},
    lessons = {
      [1] = BASE .. "Hausa_Lesson_1.mp3", [2] = BASE .. "Hausa_Lesson_2.mp3", [3] = BASE .. "Hausa_Lesson_3.mp3",
      [4] = BASE .. "Hausa_Lesson_4.mp3", [5] = BASE .. "Hausa_Lesson_5.mp3", [6] = BASE .. "Hausa_Lesson_6.mp3",
      [7] = BASE .. "Hausa_Lesson_7.mp3", [8] = BASE .. "Hausa_Lesson_8.mp3", [9] = BASE .. "Hausa_Lesson_9.mp3",
      [10] = BASE .. "Hausa_Lesson_10.mp3"
    }
  },
  yoruba = {
    section = {[1] = BASE .. "Yoruba_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "Yoruba_Section_2_lessons_6_to_10.mp3"},
    lessons = {
      [1] = BASE .. "Yoruba_Lesson_1.mp3", [2] = BASE .. "Yoruba_Lesson_2.mp3", [3] = BASE .. "Yoruba_Lesson_3.mp3",
      [4] = BASE .. "Yoruba_Lesson_4.mp3", [5] = BASE .. "Yoruba_Lesson_5.mp3", [6] = BASE .. "Yoruba_Lesson_6.mp3",
      [7] = BASE .. "Yoruba_Lesson_7.mp3", [8] = BASE .. "Yoruba_Lesson_8.mp3", [9] = BASE .. "Yoruba_Lesson_9.mp3",
      [10] = BASE .. "Yoruba_Lesson_10.mp3"
    }
  },
  igbo = {
    section = {[1] = BASE .. "Igbo_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "Igbo_Section_2_lessons_6_to_10.mp3"},
    lessons = {
      [1] = BASE .. "Igbo_Lesson_1.mp3", [2] = BASE .. "Igbo_Lesson_2.mp3", [3] = BASE .. "Igbo_Lesson_3.mp3",
      [4] = BASE .. "Igbo_Lesson_4.mp3", [5] = BASE .. "Igbo_Lesson_5.mp3", [6] = BASE .. "Igbo_Lesson_6.mp3",
      [7] = BASE .. "Igbo_Lesson_7.mp3", [8] = BASE .. "Igbo_Lesson_8.mp3", [9] = BASE .. "Igbo_Lesson_9.mp3",
      [10] = BASE .. "Igbo_Lesson_10.mp3"
    }
  },
  tiv = {
    section = {[1] = BASE .. "Tiv_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "Tiv_Section_2_lessons_6_to_10.mp3"},
    lessons = {
      [1] = BASE .. "Tiv_Lesson_1.mp3", [2] = BASE .. "Tiv_Lesson_2.mp3", [3] = BASE .. "Tiv_Lesson_3.mp3",
      [4] = BASE .. "Tiv_Lesson_4.mp3", [5] = BASE .. "Tiv_Lesson_5.mp3", [6] = BASE .. "Tiv_Lesson_6.mp3",
      [7] = BASE .. "Tiv_Lesson_7.mp3", [8] = BASE .. "Tiv_Lesson_8.mp3", [9] = BASE .. "Tiv_Lesson_9.mp3",
      [10] = BASE .. "Tiv_Lesson_10.mp3"
    }
  },
  pidgin = {
    section = {[1] = BASE .. "Pidgin_Section_1_lessons_1_to_5.mp3", [2] = BASE .. "Pidgin_Section_2_lessons_6_to_10.mp3"},
    lessons = {
      [1] = BASE .. "Pidgin_Lesson_1.mp3", [2] = BASE .. "Pidgin_Lesson_2.mp3", [3] = BASE .. "Pidgin_Lesson_3.mp3",
      [4] = BASE .. "Pidgin_Lesson_4.mp3", [5] = BASE .. "Pidgin_Lesson_5.mp3", [6] = BASE .. "Pidgin_Lesson_6.mp3",
      [7] = BASE .. "Pidgin_Lesson_7.mp3", [8] = BASE .. "Pidgin_Lesson_8.mp3", [9] = BASE .. "Pidgin_Lesson_9.mp3",
      [10] = BASE .. "Pidgin_Lesson_10.mp3"
    }
  }
}

-- ===========================
-- Utilities
-- ===========================
local function log(msg, level)
  level = level or "INFO"
  local out = string.format("%s | %s | %s", os.date("%Y-%m-%d %H:%M:%S"), level, tostring(msg))
  if level == "ERR" then freeswitch.consoleLog("ERR", "DDM_IVR: " .. tostring(msg) .. "\n") else freeswitch.consoleLog("INFO", "DDM_IVR: " .. tostring(msg) .. "\n") end
  if LOG_ENABLED then
    local fh, err = io.open(LOG_FILE, "a")
    if fh then fh:write(out .. "\n"); fh:close() else freeswitch.consoleLog("ERR", "DDM_IVR: Could not open log file: " .. tostring(err) .. "\n") end
  end
end

local function md5_hex(s)
  if not s then return "" end
  if md5_mod and md5_mod.sumhexa then return md5_mod.sumhexa(s) end
  if md5_mod and md5_mod.sum then return md5_mod.sum(s) end
  local cmd = string.format("echo -n '%s' | md5sum | awk '{print $1}'", s:gsub("'", "'\\''"))
  local f = io.popen(cmd); local out = f:read("*a") or ""; f:close()
  return out:match("(%w+)") or ""
end

local function json_encode(tbl)
  if json and json.encode then return json.encode(tbl) end
  local parts = {}
  for k,v in pairs(tbl) do
    local val = (type(v) == "number") and tostring(v) or '"'..tostring(v):gsub('"','\\"')..'"'
    table.insert(parts, '"'..tostring(k)..'":'..val)
  end
  return "{"..table.concat(parts, ",").."}"
end

local function json_decode(s)
  if not s or s == "" then return nil end
  if json and json.decode then local ok,res=pcall(json.decode,s); if ok then return res end end
  local t = {} for k,v in s:gmatch('"([^"]+)"%s*:%s*"([^"]*)"') do t[k]=v end for k,v in s:gmatch('"([^"]+)"%s*:%s*(%d+)') do t[k]=tonumber(v) end return t
end

local function normalize_msisdn(raw)
  if not raw then return nil end
  raw = tostring(raw):gsub("%s+", ""):gsub("[^%d]", "") -- remove spaces and non-digits
  if raw == "" then return nil end

  -- Remove leading '+' if present
  if raw:sub(1,1) == '+' then raw = raw:sub(2) end

  -- Already normalized
  if raw:match("^234%d+$") and #raw == 13 then return raw end

  -- 11-digit local format starting with 0
  if raw:match("^0%d%d%d%d%d%d%d%d%d%d$") then return "234" .. raw:sub(2) end

  -- 10-digit format
  if raw:match("^%d%d%d%d%d%d%d%d%d%d$") then return "234" .. raw end

  return nil
end

local function file_exists(p) local f = io.open(p,"r"); if f then f:close(); return true end; return false end

local function play_or_record(session, filepath, seconds)
  seconds = seconds or 20
  if file_exists(filepath) then session:streamFile(filepath); return true end
  log("Missing audio: " .. filepath .. " — recording caller for " .. tostring(seconds) .. "s", "ERR")
  local uuid = session:get_uuid(); local rec = BASE .. "missing_" .. uuid .. ".wav"
  os.execute(string.format("fs_cli -x 'uuid_record %s start %s' &", uuid, rec))
  if file_exists(FILES.please_wait) then session:streamFile(FILES.please_wait) end
  freeswitch.msleep(seconds*1000)
  os.execute(string.format("fs_cli -x 'uuid_record %s stop' &", uuid))
  log("Recorded missing prompt to: " .. rec)
  return false
end

-- ===========================
-- PostgreSQL connection + self-test
-- ===========================
local function connect_pgsql()
  local h = freeswitch.Dbh(pg_conn_str)
  if not h or not h:connected() then log("Postgres connection failed: " .. tostring(pg_conn_str), "ERR"); return nil end
  log("PostgreSQL connected.")
  return h
end

local dbh = connect_pgsql()
if not dbh then log("Retrying Postgres connection...", "WARN"); freeswitch.msleep(500); dbh = connect_pgsql() end
if not dbh then log("Could not connect to Postgres. Aborting.", "ERR"); if session and session:ready() then session:streamFile(FILES.system_error) end; return end

local function db_self_test()
  local ok = false
  dbh:query("SELECT 1 AS test", function(row) if tostring(row.test) == '1' then ok = true end end)
  if ok then dbh:query("SELECT version() AS ver", function(row) log("Postgres version: " .. tostring(row.ver)) end) else log("Postgres self-test failed.", "ERR") end
  return ok
end

if not db_self_test() then if session and session:ready() then session:streamFile(FILES.system_error) end return end

-- ===========================
-- To fetch resume_lesson
-- ===========================
local function get_resume_lesson(msisdn)
  local lesson = 1
  if not dbh or not dbh:connected() then return lesson end
  local q = string.format("SELECT resume_lesson FROM ddm_subscribers WHERE msisdn='%s' LIMIT 1", msisdn)
  dbh:query(q, function(row)
    if row and row.resume_lesson then
      lesson = tonumber(row.resume_lesson) or 1
    end
  end)
  return lesson
end

-- ============================
-- Resume_lesson after playback
-- ============================
local function update_resume_lesson(msisdn, lesson)
  if not dbh or not dbh:connected() then return end
  local q = string.format("UPDATE ddm_subscribers SET resume_lesson=%d, last_access=CURRENT_TIMESTAMP WHERE msisdn='%s'", lesson, msisdn)
  dbh:query(q)
end

-- ===========================
-- Ensure tables
-- ===========================
local ok, err = pcall(function()
  dbh:query([[[
    CREATE TABLE IF NOT EXISTS ddm_subscribers (
      id SERIAL PRIMARY KEY,
      msisdn VARCHAR(20) UNIQUE NOT NULL,
      caller_msisdn VARCHAR(20),
      plan VARCHAR(50),
      product_id VARCHAR(100),
      service_id VARCHAR(50),
      transaction_id VARCHAR(100),
      status VARCHAR(20) DEFAULT 'PENDING',
      subscription_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      confirmed_at TIMESTAMP,
      last_access TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  ]])

  dbh:query([[[
    CREATE TABLE IF NOT EXISTS ddm_referrals (
      id SERIAL PRIMARY KEY,
      referrer_msisdn VARCHAR(20) NOT NULL,
      referred_msisdn VARCHAR(20) NOT NULL,
      product_id VARCHAR(100),
      service_id VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  ]])
end)
if not ok then log("Error creating tables: " .. tostring(err), "ERR") end

-- ===========================
-- DB helpers
-- ===========================
local function save_subscriber(msisdn, caller_msisdn, plan, product_id, service_id, transaction_id, status, channel)
  if not dbh or not dbh:connected() then 
    log("DB not connected; cannot save subscriber.", "ERR") 
    return false 
  end

  local sql = string.format([[[
    INSERT INTO ddm_subscribers (msisdn, caller_msisdn, plan, product_id, service_id, transaction_id, status, channel, last_access)
    VALUES ('%s','%s','%s','%s','%s','%s','%s','%s',CURRENT_TIMESTAMP)
    ON CONFLICT (msisdn) DO UPDATE
      SET caller_msisdn   = EXCLUDED.caller_msisdn,
          plan            = EXCLUDED.plan,
          product_id      = EXCLUDED.product_id,
          service_id      = EXCLUDED.service_id,
          transaction_id  = EXCLUDED.transaction_id,
          status          = EXCLUDED.status,
          channel         = EXCLUDED.channel,
          last_access     = CURRENT_TIMESTAMP;
  ]], msisdn, caller_msisdn or "", plan or "", product_id or "", service_id or "", transaction_id or "", status or "PENDING", channel or "IVR")

  log("SQL Save Subscriber: " .. sql)
  local ok, err = pcall(function() dbh:query(sql) end)
  if not ok then 
    log("DB Save subscriber error: " .. tostring(err), "ERR") 
    return false 
  end

  log("Saved subscriber " .. msisdn .. " -> " .. tostring(status) .. " via " .. tostring(channel))
  return true
end

local function save_referral(referrer, referred, product_id, service_id, transaction_id, channel)
  if not dbh or not dbh:connected() then 
    log("DB not connected; cannot save referral.", "ERR") 
    return false 
  end

  local sql = string.format([[[
    INSERT INTO ddm_referrals (referrer_msisdn, referred_msisdn, product_id, service_id, transaction_id, channel)
    VALUES ('%s','%s','%s','%s','%s','%s')
  ]], referrer, referred, product_id or "", service_id or "", transaction_id or "", channel or "IVR")

  log("SQL Referral Insert: " .. sql)
  local ok, err = pcall(function() dbh:query(sql) end)
  if not ok then 
    log("DB Referral insert error: " .. tostring(err), "ERR") 
    return false 
  end

  log("Saved referral " .. referrer .. " -> " .. referred .. " via " .. tostring(channel))
  return true
end

local function is_active(msisdn)
  if not msisdn or msisdn=="" then log("is_active() called with empty msisdn", "ERR"); return false end
  if not dbh or not dbh:connected() then log("is_active(): DB not connected", "ERR"); return false end
  local found=false
  local q = string.format("SELECT msisdn, plan, product_id, status, subscription_date, last_access FROM ddm_subscribers WHERE msisdn='%s' AND status='ACTIVE' LIMIT 1", msisdn)
  log("SQL Query: " .. q)
  dbh:query(q, function(row) found = true; if json and json.encode then log("Row: " .. json.encode(row)) end end)
  if not found then log("is_active(): no active subscriber found for " .. msisdn) end
  return found
end

-- ===========================
-- Telco API call (MD5 auth + JSON POST)
-- ===========================
local function telco_subscribe(msisdn, productId, amount)
  local timeStamp = os.date("!%Y%m%d%H%M%S") -- UTC timestamp
  local spPassword = md5_hex(spId .. password .. timeStamp)
  local txn = md5_hex(msisdn .. timeStamp .. tostring(math.random(1000,9999))):sub(1,24)

  local payload_table = {
    msisdn = msisdn,
    serviceId = serviceId,
    productId = productId,
    amount = amount,
    channelId = 4,
    transactionId = txn
  }
  local payload = json_encode(payload_table)
  local esc = payload:gsub("'", "'\\''")

  local cmd = string.format("curl -s -X POST '%s' -H 'Content-Type: application/json' -H 'spId: %s' -H 'spPassword: %s' -H 'timeStamp: %s' -d '%s'",
    telco_url, spId, spPassword, timeStamp, esc)

  log("Executing Telco API: " .. cmd)
  local fh = io.popen(cmd)
  local out = fh:read("*a") or ""
  fh:close()
  log("Telco raw response: " .. out)
  local dec = json_decode(out)
  if not dec then log("Telco response JSON decode failed", "ERR") end

  return dec or {}, txn   -- return both response and txn
end

local function failure_audio_for(resp)
  local m = tostring((resp and resp.message) or ""):lower()
  if m:find("insufficient") then return FILES.failed_credit end
  return FILES.failed_tech
end

-- ===========================
-- Verify audio files (startup)
-- ===========================
local function verify_audio_files()
  local check = { FILES.welcome, FILES.subscription_menu, FILES.success_self, FILES.success_3rd, FILES.enter_3rd, FILES.language_selection, FILES.invalid, FILES.please_wait, FILES.system_error }
  for _, f in ipairs(check) do if not file_exists(f) then log("Missing audio: "..tostring(f), "WARN") else log("Found audio: "..tostring(f)) end end
  for lname,l in pairs(LANGS) do
    if not file_exists(l.section[1]) then log("Missing section file for "..lname, "WARN") end
  end
end
verify_audio_files()

-- ===========================
-- Language / Content handlers (responsive)
-- ===========================
local LANG_BY_KEY = { ["1"]="english", ["2"]="hausa", ["3"]="yoruba", ["4"]="igbo", ["5"]="tiv", ["6"]="pidgin" }

local function play_lesson(session, lang, lesson_num)
  local ln = tonumber(lesson_num)
  if not ln or ln < 1 or ln > 10 then session:streamFile(FILES.invalid); return end
  local f = LANGS[lang] and LANGS[lang].lessons[ln]
  if f and file_exists(f) then session:streamFile(f)
  -- Update resume_lesson after playback
  if caller_msisdn then
    update_resume_lesson(caller_msisdn, tonumber(lesson_num))
  end
  else session:streamFile(FILES.invalid) end
end

local function section_menu(session, lang, section)
  local caller_msisdn = session:getVariable("caller_msisdn")  -- retrieve caller ID
  section = tonumber(section) or 1
  local section_file = LANGS[lang] and LANGS[lang].section[section] or FILES.invalid
  local invalid_file = file_exists(FILES.invalid) and FILES.invalid or ""
  local last_lesson_index = nil

  log("Starting section menu with barge-in for language: " .. lang .. ", section: " .. tostring(section))

  while session:ready() do
    local ok, digit = pcall(function()
      return session:playAndGetDigits(1, 1, 3, 10000, "#", section_file, invalid_file, "^[0-9]$")
    end)

    if not ok then
      log("section_menu(): playAndGetDigits failed: " .. tostring(digit), "ERR")
      session:streamFile(FILES.invalid)
      return
    end

    digit = tostring(digit or ""):gsub("#", ""):gsub("%s+", "")
    log("User input during section menu: " .. digit)

    if digit == "" then
      log("No input received, ending section menu")
      session:streamFile(FILES.invalid)
      return

    elseif digit == "0" then
      log("User requested replay of section menu")
      -- loop continues

    elseif digit == "8" then
      if last_lesson_index then
        local lesson_file = LANGS[lang] and LANGS[lang].lessons[last_lesson_index]
        if lesson_file and file_exists(lesson_file) then
          log("Repeating last lesson " .. last_lesson_index .. " for language: " .. lang)
          session:streamFile(lesson_file)
          if caller_msisdn then
            update_resume_lesson(caller_msisdn, last_lesson_index)
          end
        else
          log("Last lesson file not found: " .. tostring(last_lesson_index), "ERR")
          session:streamFile(FILES.invalid)
        end
      else
        log("No previous lesson to repeat")
        session:streamFile(FILES.invalid)
      end

    elseif digit == "9" then
      local other = (section == 1) and 2 or 1
      log("User requested switch to section: " .. tostring(other))
      return section_menu(session, lang, other)

    else
      local lesson_num = tonumber(digit)
      if lesson_num and lesson_num >= 1 and lesson_num <= 6 then
        local lesson_index = (section - 1) * 6 + lesson_num
        local lesson_file = LANGS[lang] and LANGS[lang].lessons[lesson_index]
        if lesson_file and file_exists(lesson_file) then
          log("Playing lesson " .. lesson_index .. " for language: " .. lang)
          session:streamFile(lesson_file)
          last_lesson_index = lesson_index
          if caller_msisdn then
            update_resume_lesson(caller_msisdn, lesson_index)
          end
        else
          log("Lesson file not found for lesson " .. lesson_index .. " in language: " .. lang, "ERR")
          session:streamFile(FILES.invalid)
        end
      else
        log("Invalid lesson selection: " .. digit)
        session:streamFile(FILES.invalid)
      end
    end
  end
end

local function content_menu_with_confirmation(session, lang)
  local confirm_file = FILES.you_have_chosen[lang] or FILES.invalid
  local invalid_file = file_exists(FILES.invalid) and FILES.invalid or ""

  log("Prompting user with confirmation file for language: " .. lang)

  -- First attempt with barge-in
  local ok, selection = pcall(function()
    return session:playAndGetDigits(1, 1, 3, 10000, "#", confirm_file, invalid_file, "^[12]$")
  end)

  if not ok then
    log("playAndGetDigits failed: " .. tostring(selection), "ERR")
    session:streamFile(FILES.invalid)
    session:hangup()
    return
  end

  selection = tostring(selection or ""):gsub("#", ""):gsub("%s+", "")
  log("User selection after confirmation prompt: " .. selection)

  if selection == "1" then
    log("User selected Section 1 for language: " .. lang)
    return section_menu(session, lang, 1)
  elseif selection == "2" then
    log("User selected Section 2 for language: " .. lang)
    return section_menu(session, lang, 2)
  else
    log("Invalid input, retrying confirmation prompt")

    -- Second attempt
    local ok2, selection2 = pcall(function()
      return session:playAndGetDigits(1, 1, 3, 10000, "#", confirm_file, invalid_file, "^[12]$")
    end)

    if not ok2 then
      log("Second playAndGetDigits failed: " .. tostring(selection2), "ERR")
      session:streamFile(FILES.invalid)
      session:hangup()
      return
    end

    selection2 = tostring(selection2 or ""):gsub("#", ""):gsub("%s+", "")
    log("User selection after second attempt: " .. selection2)

    if selection2 == "1" then
      log("User selected Section 1 on second attempt")
      return section_menu(session, lang, 1)
    elseif selection2 == "2" then
      log("User selected Section 2 on second attempt")
      return section_menu(session, lang, 2)
    else
      log("No valid input after second attempt, hanging up")
      session:streamFile(FILES.invalid)
      session:hangup()
    end
  end
end

local function language_menu(session)
  local prompt = FILES.language_selection
  local invalid_file = file_exists(FILES.invalid) and FILES.invalid or ""
  local attempts = 0
  local lang = nil

  while attempts < 2 and session:ready() do
    log("language_menu(): Attempt " .. (attempts + 1))

    local ok, choice = pcall(function()
      return session:playAndGetDigits(1, 1, 1, 15000, "#", prompt, "", "^[1-6]$")
    end)

    local input = (ok and choice) and tostring(choice):gsub("#", ""):gsub("%s+", "") or ""

    if input == "" then
      log("language_menu(): No input received on attempt " .. (attempts + 1))
      session:streamFile(invalid_file)
      attempts = attempts + 1
    else
      lang = LANG_BY_KEY[input]
      if lang then
        log("Language selected: " .. lang)
        break
      else
        log("Invalid language selection: " .. input .. " on attempt " .. (attempts + 1))
        session:streamFile(invalid_file)
        attempts = attempts + 1
      end
    end
  end

  if not lang then
    log("No valid language selected after " .. attempts .. " attempts")
    lang = "english"
    log("Defaulting to English")
  end

  local confirm_file = FILES.you_have_chosen[lang]
  if confirm_file and file_exists(confirm_file) then
    session:streamFile(confirm_file)
  end

  content_menu_with_confirmation(session, lang)
end

-- ===========================
-- 3rd-party capture helper
-- ===========================
local function collect_msisdn(session, prompt_file)
  local min_digits, max_digits = 7, 15
  local timeout_ms = 30000
  local terminators = "#"
  local prompt = prompt_file or FILES.enter_3rd
  local invalid_file = file_exists(FILES.invalid) and FILES.invalid or ""
  local ok, digits = pcall(function()
    return session:playAndGetDigits(min_digits, max_digits, 3, timeout_ms, terminators, prompt, invalid_file, "")
  end)
  if not ok or not digits or digits == "" then
    local ok2, got = pcall(function() return session:getDigits(1, terminators, timeout_ms) end)
    if ok2 and got and got ~= "" then digits = got end
  end
  if not digits or digits == "" then return nil, nil end

  digits = tostring(digits):gsub("#",""):gsub("%s+","")
  local normalized = normalize_msisdn(digits)
  return digits, normalized  -- return both raw and normalized
end

local function collect_and_confirm_3rd_party_number(session)
  local attempts = 0
  while attempts < 3 and session:ready() do
    attempts = attempts + 1
    local raw_digits, normalized = collect_msisdn(session, FILES.enter_3rd)
    if not normalized then
      log("3rd-party number capture failed or invalid on attempt " .. attempts, "WARN")
      session:streamFile(FILES.invalid)
      goto continue
    end

    session:streamFile(FILES.you_have_entered)
    local ok, err = pcall(function()
      session:execute("say", "en number iterated " .. tostring(raw_digits))
    end)
    if not ok then log("say failed: " .. tostring(err), "WARN") end

    session:streamFile(FILES.confirm_prompt)
    local confirm = session:getDigits(1, "", 8000)
    confirm = tostring(confirm or ""):gsub("%s+", "")

    if confirm == "1" then
      return raw_digits, normalized
    elseif confirm == "2" then
      log("3rd-party number re-enter requested (attempt " .. attempts .. ")")
      -- retry
    else
      session:streamFile(FILES.invalid)
      log("3rd-party number confirmation invalid: " .. tostring(confirm), "WARN")
    end

    ::continue::
  end
  return nil, nil
end

-- ===========================
-- Main IVR flow
-- ===========================
if not session:ready() then return end
session:answer()

local raw = session:getVariable("caller_id_number") or session:getVariable("caller_id_name") or session:getVariable("sip_from_user") or ""
log("Incoming raw caller: " .. tostring(raw))
local caller_msisdn = normalize_msisdn(raw)
if not caller_msisdn then caller_msisdn = normalize_msisdn(session:getVariable("caller_id_name") or "") end
if not caller_msisdn then log("Could not normalize caller id", "ERR"); session:streamFile(FILES.failed_tech); session:hangup(); return end
session:setVariable("caller_msisdn", caller_msisdn)
log("Normalized caller_msisdn: " .. caller_msisdn)

-- Resume lesson if already active
if is_active(caller_msisdn) then
  log("Active subscriber detected: " .. caller_msisdn .. " -> routing to resume prompt")
  if dbh and dbh:connected() then
    dbh:query("UPDATE ddm_subscribers SET last_access=CURRENT_TIMESTAMP WHERE msisdn='"..caller_msisdn.."'")
  end

  local resume_lesson = get_resume_lesson(caller_msisdn)
  local prompt = "flite|kal16|Welcome, you last listened to Lesson " .. resume_lesson ..
                 ". Press 1 to continue from Lesson " .. (resume_lesson + 1) ..
                 ", or press 2 to start over."

  session:execute("speak", prompt)
  local choice = session:getDigits(1, "#", 8000)
  choice = tostring(choice or ""):gsub("#", ""):gsub("%s+", "")

  if choice == "1" then
    local next_lesson = resume_lesson + 1
    play_lesson(session, "english", next_lesson)
    update_resume_lesson(caller_msisdn, next_lesson)
    return section_menu(session, "english", (next_lesson <= 5) and 1 or 2)
  elseif choice == "2" then
    play_lesson(session, "english", 1)
    update_resume_lesson(caller_msisdn, 1)
    return section_menu(session, "english", 1)
  end

  -- fallback to last-used language confirmation
  local last_lang = "english"
  local confirm_file = FILES.you_have_chosen[last_lang]
  if confirm_file and file_exists(confirm_file) then
    session:streamFile(confirm_file)
  end
  content_menu_with_confirmation(session, last_lang)
  return
end

-- Who (self / 3rd-party)
play_or_record(session, FILES.welcome)
local who = session:getDigits(1, "", 8000)
if who ~= "1" and who ~= "2" then session:streamFile(FILES.invalid); session:hangup(); return end

-- subscription menu -> plan_choice
local invalid_file = file_exists(FILES.invalid) and FILES.invalid or ""
local attempts = 0
local plan_choice = nil

while attempts < 2 and session:ready() do
  local ok, choice = pcall(function()
    return session:playAndGetDigits(1, 1, 3, 10000, "#", FILES.subscription_menu, invalid_file, "^[1-5]$")
  end)

  if not ok then
    log("Subscription menu playAndGetDigits failed: " .. tostring(choice), "ERR")
    session:streamFile(FILES.invalid)
    attempts = attempts + 1
  else
    choice = tostring(choice or ""):gsub("#", ""):gsub("%s+", "")
    if PRODUCTS[tonumber(choice)] then
      plan_choice = choice
      log("User selected plan: " .. plan_choice)
      break
    else
      log("Invalid plan selection: " .. tostring(choice))
      session:streamFile(FILES.invalid)
      attempts = attempts + 1
    end
  end
end

if not plan_choice then
  log("No valid plan selected after " .. attempts .. " attempts")
  session:streamFile(FILES.invalid)
  session:hangup()
  return
end

local prod = PRODUCTS[tonumber(plan_choice)]

-- play DOI prompt and confirm
play_or_record(session, FILES.doi[prod.amount])
local doi_conf = session:getDigits(1, "", 7000)
if doi_conf ~= "1" then session:streamFile(FILES.cancelled); session:hangup(); return end

local target_msisdn = caller_msisdn
local referred_msisdn = nil
if who == "2" then
  local raw_digits, normalized = collect_and_confirm_3rd_party_number(session)
  if not normalized then
    session:streamFile(FILES.invalid)
    session:hangup()
    return
  end
  referred_msisdn = normalized
  target_msisdn = normalized
end

-- call telco API (charging request)
local resp, txn = telco_subscribe(caller_msisdn, prod.productId, prod.amount)
local status_code = tonumber(tostring(resp.status or resp.code or "0"))
log("Telco response interpreted status: " .. tostring(status_code))

if status_code ~= 200 then
  -- Network failure branch
  play_or_record(session, FILES.network_failed)
  log("Network failure during subscription: " .. tostring(resp.message or json_encode(resp)), "ERR")
  session:hangup()
  return
else
  -- Success branch: save subscriber/referral correctly
  if who == "1" then
    -- SELF subscription
    save_subscriber(
      caller_msisdn,          -- msisdn (self)
      caller_msisdn,          -- caller_msisdn
      prod.plan,
      prod.productId,
      serviceId,
      txn,
      "PENDING"
    )
    -- Send consent SMS to self subscriber
    local sms = require "ddm.mtsms"
    local ok_consent, resp_consent = sms.send({
      msisdn   = caller_msisdn,
      message  = sms.msg_consent_request(),
      messageId= "CONSENT-" .. txn
    })
    if not ok_consent then
      log("SMS(consent self) failed: " .. json_encode(resp_consent), "ERR")
    end

  elseif who == "2" then
    -- THIRD-PARTY subscription
    -- Save referral with transaction_id
    save_referral(
      caller_msisdn,
      referred_msisdn,
      prod.productId,
      serviceId,
      txn
    )

    -- Save referred subscriber with sponsored number as msisdn
    save_subscriber(
      referred_msisdn,
      caller_msisdn,
      prod.plan,
      prod.productId,
      serviceId,
      txn,
      "PENDING"
    )

    -- Send consent SMS to referred subscriber
    local sms = require "ddm.mtsms"
    local ok_consent, resp_consent = sms.send({
      msisdn   = referred_msisdn,
      message  = sms.msg_consent_request(),
      messageId= "CONSENT-" .. txn
    })
    if not ok_consent then
      log("SMS(consent referred) failed: " .. json_encode(resp_consent), "ERR")
    end

    -- Also notify sponsor that consent is required
    sms.send({
      msisdn   = caller_msisdn,
      message  = "Consent required for " .. referred_msisdn ..
                 ": please ensure they dial *480*3#.",
      messageId= "CONSENT-SP-" .. txn
    })
  end
end

-- Store FreeSWITCH UUID for callback correlation
local uuid = session:get_uuid()
dbh:query(string.format(
  "UPDATE ddm_subscribers SET freeswitch_uuid='%s' WHERE transaction_id='%s'",
  uuid, txn
))

-- Play background music while waiting for callback
play_or_record(session, FILES.subscription_bgm)

-- Poll subscriber status until callback updates it
local function wait_for_callback(txn, is_3pp)
  local status = "PENDING"
  while session:ready() do
    local q = string.format("SELECT status FROM ddm_subscribers WHERE transaction_id='%s' LIMIT 1", txn)
    dbh:query(q, function(row)
      if row and row.status then
        status = tostring(row.status)
      end
    end)

    if status == "ACTIVE" then
      log("Callback confirmed subscription ACTIVE for txn " .. txn)
      if is_3pp then
        play_or_record(session, FILES.thank_you_referral)
        session:hangup()
        return true
      else
        play_or_record(session, FILES.success_self)
        language_menu(session)
        session:hangup()
        return true
      end

    elseif status == "FAILED" then
      log("Callback confirmed subscription FAILED for txn " .. txn)
      play_or_record(session, FILES.failed_subscription)
      session:hangup()
      return false
    end

    freeswitch.msleep(2000) -- poll every 2s
  end
  return false
end

local is_3pp = (who == "2")
wait_for_callback(txn, is_3pp)

