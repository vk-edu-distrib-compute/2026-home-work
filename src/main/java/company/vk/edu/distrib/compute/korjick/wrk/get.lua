counter = 0

local ack = os.getenv("ACK")
local key_prefix = os.getenv("KEY_PREFIX") or "key"
local key_space = tonumber(os.getenv("KEY_SPACE") or "100000")

local function path(key)
  local result = "/v0/entity?id=" .. key
  if ack ~= nil and ack ~= "" then
    result = result .. "&ack=" .. ack
  end
  return result
end

request = function()
  counter = (counter % key_space) + 1
  return wrk.format("GET", path(key_prefix .. counter))
end
