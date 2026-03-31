-- Инициализация генератора случайных чисел,
-- чтобы GET-запросы пробегались не только по
-- созданным PUT-запросами файлам
math.randomseed(1)

request = function()
    local id = tostring(math.random(1, 5000))
    local path = "/v0/entity?id=" .. id
    return wrk.format("GET", path)
end