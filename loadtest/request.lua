local counter = 0
local body = string.rep("x", 1024)
local keyCount = tonumber(os.getenv("LOAD_KEYS") or "2000")

request = function()
    counter = counter + 1
    local key = "cluster-" .. ((counter - 1) % keyCount)

    if counter <= keyCount or counter % 5 == 0 then
        return wrk.format("PUT", "/v0/entity?id=" .. key, nil, body)
    end

    return wrk.format("GET", "/v0/entity?id=" .. key)
end
