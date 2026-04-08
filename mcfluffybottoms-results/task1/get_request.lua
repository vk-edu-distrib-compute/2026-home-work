wrk.method = "GET"
wrk.headers["Content-Type"] = "application/json"

request = function()
    local id = math.random(1, 1000000)
    return wrk.format("GET", "/v0/entity/" .. id, nil)
end