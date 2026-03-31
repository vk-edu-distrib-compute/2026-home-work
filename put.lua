request = function()
    local id = tostring(math.random(1, 5000))
    local path = "/v0/entity?id=" .. id
    local body = "data_" .. id
    return wrk.format("PUT", path, nil, body)
end