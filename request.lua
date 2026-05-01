local counter = 0

request = function()
    counter = counter + 1

    local url = "http://host.docker.internal:8080/v0/entity?id=" .. counter

    return wrk.format("PUT", url)
end

