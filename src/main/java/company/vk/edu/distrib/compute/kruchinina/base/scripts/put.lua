local counter = 0

function request()
    path = "/v0/entity?id=test_key_" .. counter
    counter = counter + 1
    body = "some interesting data " .. counter
    return wrk.format("PUT", path, nil, body)
end
