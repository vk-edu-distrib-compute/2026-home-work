wrk.method = "PUT"
wrk.headers["Content-Type"] = "application/json"

request = function()
    local id = math.random(1, 1000000)
    local body = string.format('{"value": "data_%d"}', id)
    return wrk.format("PUT", "/v0/entity/" .. id, nil, body)
end