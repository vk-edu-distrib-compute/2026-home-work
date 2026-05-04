local counter = 0
local keyCount = tonumber(os.getenv("LOAD_KEYS") or "6000")
local firstKey = tonumber(os.getenv("LOAD_FIRST_KEY") or "2")

request = function()
    counter = (counter % keyCount) + 1
    local path = "/v0/entity?id=load-" .. (firstKey + counter - 1)
    return wrk.format("GET", path)
end
