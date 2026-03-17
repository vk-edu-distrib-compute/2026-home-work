# 2026-home-work 
Домашние задание курса "Распределенные вычисления" 2026 года

## Задание 1. HTTP API + хранилище (deadline 1 неделя)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2026-home-work.git
Cloning into '2026-home-work'...
remote: Counting objects: 34, done.
remote: Compressing objects: 100% (24/24), done.
remote: Total 34 (delta 2), reused 34 (delta 2), pack-reused 0
Receiving objects: 100% (34/34), 11.43 KiB | 3.81 MiB/s, done.
Resolving deltas: 100% (2/2), done.
$ git remote add upstream git@github.com:vk-edu-distrib-compute/2026-home-work.git
$ git fetch upstream
From github.com:vk-edu-distrib-compute/2026-home-work
 * [new branch]      master     -> upstream/master
```

### Test
Так можно запустить тесты:
```
$ ./gradlew check
```

### Run
А вот так -- сервер:
```
$ ./gradlew run
```

### Develop
Откройте в IDE -- [IntelliJ IDEA](https://www.jetbrains.com/idea/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx128m`. 

В своём Java package `company.vk.edu.distrib.compute.<username>` реализуйте интерфейс [`KVService`](src/main/java/company/vk/edu/distrib/compute/KVService.java) и поддержите следующий HTTP REST API протокол:
* HTTP `GET /v0/entity?id=<ID>` -- получить данные по ключу `<ID>`. Возвращает `200 OK` и данные или `404 Not Found`.
* HTTP `PUT /v0/entity?id=<ID>` -- создать/перезаписать (upsert) данные по ключу `<ID>`. Возвращает `201 Created`.
* HTTP `DELETE /v0/entity?id=<ID>` -- удалить данные по ключу `<ID>`. Возвращает `202 Accepted`.

Возвращайте реализацию интерфейса в [`KVServiceFactory`](src/main/java/company/vk/edu/distrib/compute/KVServiceFactory.java).
Для первого задания достаточно хранить данные в памяти. Задание со звездочкой - хранить данные на диске.

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). 
Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!