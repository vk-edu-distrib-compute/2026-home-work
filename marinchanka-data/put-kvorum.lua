counter = 0
request = function()
    counter = counter + 1
    return wrk.format("PUT", "/v0/entity?id=key" .. counter .. "&ack=2", {}, "value" .. counter)
end
