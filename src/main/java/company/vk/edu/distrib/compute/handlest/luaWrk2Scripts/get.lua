counter = 0

request = function()
    counter = counter + 1
    local id = "key_" .. counter
    local path = "/v0/entity?id=" .. id
    return wrk.format("GET", path)
end