local method = "PUT"
local path = "/v0/entity?id="
local headers = { ["Content-Type"] = "application/json" }
local body = '{"payload": "data"}'
local counter = 0

request = function()
    counter = counter + 1
    return wrk.format(method, path .. counter, headers, body)
end
