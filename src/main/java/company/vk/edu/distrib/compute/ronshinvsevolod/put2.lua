counter = 0

function request()
    path = "/v0/entity?id=key_" .. counter
    counter = counter + 1
    return wrk.format("PUT", path, nil, "test_value")
end