---
--- Created by vodobryshkin
--- DateTime: 01.04.2026 14:03
---

function randomString()
    local length = math.random(1, 5);
    local charset = "abcde"
    local result = ""
    for i = 1, length do
        local randIndex = math.random(1, #charset)
        result = result .. charset:sub(randIndex, randIndex)
    end
    return result
end

function request()
    local key = randomString()
    local body = randomString()

    return wrk.format("PUT", "/v0/entity?id=" .. key, nil, body)
end
