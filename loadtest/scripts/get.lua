wrk.method = "GET"

local counter = 0
local max_keys = 5000

request = function()
    counter = counter + 1

    local key_number = ((counter - 1) % max_keys) + 1
    local key = "key-" .. key_number
    local path = "/v0/entity?id=" .. key

    return wrk.format("GET", path)
end
