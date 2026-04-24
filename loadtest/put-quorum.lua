wrk.method = "PUT"
wrk.body   = "value"
wrk.headers["Content-Type"] = "application/octet-stream"

counter = 0
request = function()
    counter = counter + 1
    return wrk.format(nil, "/v0/entity?id=key" .. counter .. "&ack=2")
end
