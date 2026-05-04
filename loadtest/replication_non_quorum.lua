local counter = 0
local currentKey = nil
local ackWrite = tonumber(os.getenv("ACK_WRITE") or "1")
local ackRead = tonumber(os.getenv("ACK_READ") or "1")
local bodyPrefix = os.getenv("BODY_PREFIX") or "non-quorum-value-"
local keyPrefix = os.getenv("KEY_PREFIX") or "non-quorum-key-"

request = function()
    counter = counter + 1
    local pair = math.floor((counter + 1) / 2)

    if counter % 2 == 1 then
        currentKey = keyPrefix .. pair
        return wrk.format(
            "PUT",
            "/v0/entity?id=" .. currentKey .. "&ack=" .. ackWrite,
            nil,
            bodyPrefix .. pair
        )
    end

    return wrk.format("GET", "/v0/entity?id=" .. currentKey .. "&ack=" .. ackRead)
end
