counter = 0
max_keys = 10000

request = function()
    key_id = counter % max_keys
    path = "/v0/entity?id=key_" .. key_id
    counter = counter + 1
    return wrk.format("GET", path)
end
