wrk.method = "GET"
wrk.headers["Content-Type"] = "text/plain"

function request()
    local id = math.random(1, 1000000)
    local path = "/v0/entity?id=" .. id

    return wrk.format(wrk.method, path, nil, body)
end