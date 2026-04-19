wrk.method = "GET"

request = function()
    local id = math.random(1, 7000)
    local path = "/v0/entity?id=" .. id
    return wrk.format(nil, path)
end