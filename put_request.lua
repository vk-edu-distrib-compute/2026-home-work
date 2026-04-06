counter = 0

request = function()
    path = "/v0/entity?id=key_" .. counter
    counter = counter + 1
    body = "test data for key " .. counter
    return wrk.format("PUT", path, nil, body)
end
