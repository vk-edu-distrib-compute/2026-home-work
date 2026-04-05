wrk.method = "PUT"

local counter = 0

request = function()
    counter = counter + 1

    local key = "key-" .. counter
    local value = "value-" .. counter
    local path = "/v0/entity?id=" .. key

    return wrk.format("PUT", path, nil, value)
end
