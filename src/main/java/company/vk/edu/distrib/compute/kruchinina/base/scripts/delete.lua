local counter = 0

function request()
    path = "/v0/entity?id=test_key_" .. counter
    counter = counter + 1
    return wrk.format("DELETE", path, nil, nil)
end
