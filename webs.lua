dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local exitgrab = false
local exit_url = false

local outlinks = {}
local discovered = {}

local bad_items = {}

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(abort)
  if abort then
    abortgrab = true
  end
  exitgrab = true
  if not bad_items[item_name] then
    io.stdout:write("Aborting item " .. item_name .. ".\n")
    io.stdout:flush()
    bad_items[item_name] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if string.find(url, "{imageSize}")
    or string.match(url, "/apps/auth/login")
    or string.match(url, "/apps/auth/signup")
    or string.match(url, "^https?://dynamic%.websimages%.com/s/themes/[0-9a-f]+/v[0-9]+%.[0-9]+$")
    or string.match(url, "/showEvent.*[%?&]next=")
    or not (
      string.match(url, "^https?://[^/]*webs[^/]+/")
      or string.match(url, "^https?://[^/]*thum%.io/")
      or string.match(url, "^https?://[^/]*vpweb[^/]+/")
      or string.match(url, "^https?://[^/]*cloudfront%.net/")
      or string.match(url, "^https?://[^/]*vid%.ly/")
      or string.match(url, "^https?://[^/]*vistaprint[^/]+/")
      or string.match(url, "^https?://[^/]*cimpress%.io/")
      or ids[string.match(url, "^https?://([^/]+)")]
    ) then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  local cal_id = string.match(url, "[%?&]calID=([0-9]+)")
  if cal_id then
    if string.match(url, "[%?&]day=[0-9]+") then
      return false
    end
    local cal_year = tonumber(string.match(url, "[%?&]year=([0-9]+)"))
    if cal_year and (cal_year < 2000 or cal_year > 2021) then
      return false
    end
  end

  if string.match(url, "^https?://[^/]*websimages%.com/")
    or string.match(url, "^https?://[^/]*digital%.vistaprint%.com/")
    or string.match(url, "^https?://thumbs%.webs%.com/")
    or string.match(url, "^https?://images%.webs%.com/")
    or string.match(url, "^https?://thumbs%.freewebs%.com/")
    or string.match(url, "^https?://images%.freewebs%.com/")
    or string.match(url, "^https?://memberfiles%.freewebs%.com/")
    or string.match(url, "^https?://webzoom%.freewebs%.com/")
    or string.match(url, "^https?://thum%.io/get/")
    or string.match(url, "^https?://[^/]*cimpress%.io/") then
    return true
  end

  -- extra for freewebs.com to not miss anything
  if string.match(url, "^https?://[^/]+%.freewebs%.com/")
    and not string.match(url, "^https?://www%.") then
    if parenturl
      and string.match(parenturl, "^https?://[^/]+%.freewebs%.com/") then
      return false
    end
    return true
  end

  for s in string.gmatch(url, "([0-9]+)") do
    if ids[s] then
      return true
    end
  end

  if parenturl
    and (
      string.match(parenturl, "%.[jJ][pP][eE]?[gG]$")
      or string.match(parenturl, "%.[pP][nN][gG]$")
      or string.match(parenturl, "%.[gG][iI][fF]$")
      or string.match(parenturl, "%.[mM][pP]3$")
      or string.match(parenturl, "%.[pP][dD][fF]$")
      or string.match(parenturl, "%.[mM][pP]4$")
      or string.match(parenturl, "%.[zZ][iI][pP]$")
      or string.match(parenturl, "%.[pP][dD][fF]$")
    ) then
    return false
  end

  for s in string.gmatch(url, "([a-zA-Z0-9]+)") do
    if ids[s] then
      return true
    end
  end

  if string.find(url, "{imageSize}") then
    return false
  end

  local id = string.match(url, "^https?://([^/]+)%.webs%.com")
  if not id then
    id = string.match(url, "^https?://([^/]+)%.vpweb%.com")
  end
  if not id then
    id = string.match(url, "^https?://[^/]*webs%.com/([^/]+)")
  end
  if id and ids[id] then
    return true
  end

  if ids[string.match(url, "^https?://([^/]+)")] then
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local parenturl = parent["url"]

  url = string.gsub(url, ";jsessionid=[0-9A-F]+", "")

  if downloaded[url] or addedtolist[url] then
    return false
  end

  if allowed(url) or urlpos["link_expect_html"] == 0 then
    addedtolist[url] = true
    return true
  end
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  if is_css then
    return urls
  end
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.gsub(url_, ";jsessionid=[0-9A-F]+", "")
    local match = string.match(url_, "^(.+/showEvent.*[%?&])next=[^%?&]+[%?&]?")
    if match then
      url_ = match
    end
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    --url_ = string.match(url_, "^(.-)/?$")
    url_ = string.match(url_, "^(.-)\\?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^/>")
      or string.match(newurl, "^/&gt;")
      or string.match(newurl, "^/<")
      or string.match(newurl, "^/&lt;")
      or string.match(newurl, "^/%*") then
      return false
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checkfind(s)
    if s ~= "members" and s ~= "thumbs" and s ~= "images" and s ~= "www" then
      io.stdout:write("Found " .. s .. ".\n")
      ids[s] = true
      check("https://" .. s .. ".webs.com/")
    end
  end

  local a, b, c = string.match(url, "^(https?://[^/]+/.-)(https?://)(.+)$")
  if a and b and c then
    check(a .. c)
    check(b .. c)
  end

  local match = string.match(url, "^https?://[^/]*websimages%.com/fit/[^/]+/(.+)$")
  if not match then
    match = string.match(url, "^https?://[^/]*websimages%.com/thumb/[0-9]+/(.+)$")
  end
  if not match then
    match = string.match(url, "^https?://[^/]*websimages%.com/width/[0-9]+/crop/[^/]+/(.+)$")
  end
  if match then
    if not string.match(match, "^https?://") then
      match = "https://" .. match
    end
    check(match)
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://[^/]*websimages%.com/")
    and not string.match(url, "^https?://[^/]*digital%.vistaprint%.com/")
    and not string.match(url, "^https?://thumbs%.webs%.com/")
    and not string.match(url, "^https?://images%.webs%.com/")
    and not string.match(url, "^https?://thumbs%.freewebs%.com/")
    and not string.match(url, "^https?://images%.freewebs%.com/")
    and not string.match(url, "^https?://[^%.]+%.cloudfront%.net/")
    and not string.match(url, "^https?://[^/]*cimpress%.io/") then
    html = read_file(file)
    if not string.match(url, "^https?://[^/]*webs%.com")
      and not string.match(url, "^https?://[^/]*vpweb%.com")
      and string.match(url, "^https?://[^/]+/?$") then
      if not string.find(html, "webs.stats") then
        return urls
      end
      for s in string.gmatch(html, "([a-zA-Z0-9%-_]+)%.webs%.com") do
        checkfind(s)
      end
      for s in string.gmatch(html, "([a-zA-Z0-9%-_]+)%.vpweb%.com") do
        checkfind(s)
      end
      io.stdout:flush()
    end
    if string.match(url, "/robots%.txt$") then
      for line in string.gmatch(html, "([^\n]+)") do
        newurl = string.match(line, "^[^:]+: (.+)$")
        if newurl then
          checknewurl(newurl)
        end
      end
    end
    local match = string.match(html, 'data%-image%-url="([^"]+)"')
    if match then
      check("https://imageprocessor.websimages.com/fit/425x425/" .. match)
    end
    match = string.match(url, "^https?://s%.vid%.ly/embeded%.html.+link=([a-zA-Z0-9]+)")
    if match then
      check("https://vid.ly/" .. match)
      check("https://vid.ly/" .. match .. "/embed")
    end
    match = string.match(html, "vid%.ly/embeded%.html.+link=([a-zA-Z0-9]+)")
    if match then
      ids[match] = true
    end
    for file_id in string.gmatch(html, 'file[iI][dD]"?%s*[:%-=]%s*([0-9]+)') do
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id)
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=square")
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=thumb")
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=full")
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=normal")
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=large")
      check("https://thumbs.webs.com/Members/viewThumb.jsp?siteId=" .. item_value .. "&fileID=" .. file_id .. "&size=large&angle=0")
      check("https://profiles.members.webs.com/Members/viewThumb.jsp?fileID=" .. file_id)
    end
    if string.match(url, "^https?://profiles%.members%.webs%.com/Profile/index%.jsp%?userID=[0-9]+$") then
      local protocol, location = string.match(html, "document%.location='(https?://)([^']-)/?';")
      if not location then
        io.stdout:write("Could not find location.\n")
        io.stdout:flush()
        --abort_item()
        return {}
      end
      if string.find(location, "/") then
        io.stdout:write("Location is not a domain.\n")
        io.stdout:flush()
        --abort_item()
        return {}
      end
      if location == "webs.com" or location == "www.webs.com" then
        io.stdout:write("No site found.\n")
        io.stdout:flush()
        return urls
      end
      local id = string.match(location, "^(.+)%.webs%.com$")
      if not id then
        id = string.match(location, "^(.+)%.vpweb%.com$")
      end
      if id then
        ids[id] = true
      else
        print('setting', location)
        ids[location] = true
        local match = string.match(location, "^www%.(.+)$")
        if match then
          print('setting', match)
          ids[match] = true
        end
      end
      if id then
        check(protocol .. location .. "/apps/members/")
        check(protocol .. location .. "/")
        check(protocol .. location .. "/robots.txt")
        check(protocol .. location .. "/sitemap.xml")
        if id then
          check("https://members.webs.com/s/signup/checkUsername?username=" .. id)
          check("https://freewebs.com/" .. id)
        end
        check("http://profiles.members.webs.com/Members/viewProfileImage.jsp?userID=" .. item_value)
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ':%s*url%(([^%)"]+)%)') do
      checknewurl(newurl)
    end
  end

  return urls
end

set_new_item = function(url)
  local match = string.match(url, "^https?://profiles%.members%.webs%.com/Profile/index%.jsp%?userID=([0-9]+)$")
  local type_ = "site"
  if match and not ids[match] then
    abortgrab = false
    exitgrab = false
    ids[match] = true
    item_value = match
    item_type = type_
    item_name = type_ .. ":" .. match
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
end

wget.callbacks.write_to_warc = function(url, http_stat)
  set_new_item(url["url"])
  if http_stat["statcode"] == 410
    and (
      string.match(url["url"], "^https?://[^%.]+%.webs%.com/$")
      or string.match(url["url"], "^https?://[^%.]+%.webs%.com/robots%.txt$")
      or string.match(url["url"], "^https?://[^%.]+%.webs%.com/sitemap%.xml$")
    ) then
    --abort_item()
  end
  if exitgrab then
    io.stdout:write("Not writing WARC record.\n")
    io.stdout:flush()
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if abortgrab then
    abort_item(true)
    return wget.actions.ABORT
    --return wget.actions.EXIT
  end

  set_new_item(url["url"])
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  --[[if url_count > 100000 then
    return wget.actions.EXIT
  end]]

  if exitgrab then
    return wget.actions.EXIT
  end

  if string.match(url["url"], "^https?://[^/]+/?$") and status_code >= 400 then
    if status_code == 404 then
      -- okay so if this occurs it is USUALLY the fact website doesnt exist/has been deleted. 
	  -- This is fine and IF it somehow catches this not on the home page, it will still grab anything it can.
	  -- The item then gets yeeted into webs:prio:404:todo for EggplantN/others to validate later if they wish
      io.stdout:write("Got 404 status code on website front page. Website likely removed\n")
      io.stdout:flush()
      removed_site = true
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird.arpa.li:23038/webs404-k3s9p6kdrv4q556x/",
          item_name
        )
        if code == 200 or code == 409 then
          break
        end
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      --[[if tries == 10 then
        abortgrab = true
      end]]
      return wget.actions.EXIT
    else
      io.stdout:write("Got bad status code on website front page.\n")
      io.stdout:flush()
      return wget.actions.EXIT
      --abort_item(true)
    end
  end
  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc] then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if status_code == 400
    and string.match(url["url"], "^https?://mediaprocessor%.websimages%.com/") then
    return wget.actions.EXIT
  end

  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    if removed_site == true then
      return wget.actions.EXIT
    end
	-- okay so if this occurs it is USUALLY the fact DNS doesnt exist for that domain. 
	-- This is fine and IF it somehow catches this not on the home page, it will still grab anything it can.
	-- The item then gets yeeted into webs:prio:nxdomain:todo for EggplantN/others to validate later
	-- yes there is repetition of the backfeed code, i'm not well versed with LUA enough to willingly try it
	-- Example item site:133428883 -> 92ddef45-532f-4bcf-ae97-1fed4719201e.vpweb.co.uk 
	-- Example item site:138749897 -> 166d0727-96bb-434f-afc2-c8db387a47d7.vpweb.dk
	-- Example item site:136141949 -> www.ilbossdelpollo.com
	if status_code == 0 and err == "HOSTERR" then
	  io.stdout:write("Got 0 status code on website front page. With HOSTERR, likely DNS record has been removed\n")
      io.stdout:flush()
      removed_site = true
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird.arpa.li:23038/websnxdomain-z964z6wt9fcqsh5m/",
          item_name
        )
        if code == 200 or code == 409 then
          break
        end
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      --[[if tries == 10 then
        abortgrab = true
      end]]
      return wget.actions.EXIT
	end
	--[[ THIS IS NOT READY YET, FURTHER TESTING REQUIRED
	-- okay so if this occurs it is USUALLY the fact DNS doesnt exist for that domain. 
	-- This is fine and IF it somehow catches this not on the home page, it will still grab anything it can.
	-- The item then gets yeeted into webs:prio:connerr:todo for EggplantN/others to validate later
	--
	if status_code == 0 and err == "CONNIMPOSSIBLE" then
	  io.stdout:write("Got 0 status code on website front page. With CONNIMPOSSIBLE, likely web server not responding via HTTPS or at all\n")
      io.stdout:flush()
      removed_site = true
      local tries = 0
        while tries < 10 do
          local body, code, headers, status = http.request(
            "http://blackbird.arpa.li:23038//",
            item_name
          )
          if code == 200 or code == 409 then
            break
          end
          os.execute("sleep " .. math.floor(math.pow(2, tries)))
          tries = tries + 1
        end
        if tries == 10 then
          abortgrab = true
        end
      return wget.actions.EXIT
	end --]]
    local maxtries = 1
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if not allowed(url["url"], nil) then
        return wget.actions.EXIT
      end
      if string.match(url["url"], "^https?://static%.websimages%.com/")
        or string.match(url["url"], "^https?://dynamic%.websimages%.com/") then
        return wget.actions.EXIT
      end
      local match = string.match(url["url"], "^https?://([^/]+)%.freewebs%.com")
      if match and match ~= "images" and match ~= "thumbs"
        and match ~= "webzoom" and match ~= "memberfiles" then
        return wget.actions.EXIT
      end
      --abort_item(true)
      --return wget.actions.ABORT
      return wget.actions.EXIT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
    return wget.exits.IO_FAIL
  end
  return exit_status
end

