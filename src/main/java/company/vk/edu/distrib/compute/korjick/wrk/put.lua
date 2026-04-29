counter = 0

local ack = os.getenv("ACK")
local key_prefix = os.getenv("KEY_PREFIX") or "key"
local key_space = tonumber(os.getenv("KEY_SPACE") or "100000")
local value_size = tonumber(os.getenv("VALUE_SIZE") or "128")

local function path(key)
  local result = "/v0/entity?id=" .. key
  if ack ~= nil and ack ~= "" then
    result = result .. "&ack=" .. ack
  end
  return result
end

request = function()
  counter = (counter % key_space) + 1
  local key = key_prefix .. counter
  local body = string.rep("x", value_size)
  return wrk.format(
    "PUT",
    path(key),
    { ["Content-Type"] = "text/plain" },
    body
  )
end
