counter = 0

request = function()
    counter = counter + 1
    local id = "key_" .. counter
    local body = string.format('{"value": "data_for_%s", "timestamp": %d}', id, os.time())
    local path = "/v0/entity?id=" .. id
    return wrk.format("PUT", path, nil, body)
end