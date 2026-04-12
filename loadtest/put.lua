local counter = 0
local body = string.rep("x", 1024)

request = function()
    counter = counter + 1
    local path = "/v0/entity?id=load-" .. counter
    return wrk.format("PUT", path, nil, body)
end
