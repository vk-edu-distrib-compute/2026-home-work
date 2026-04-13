# ДЗ 1 (jokeryga) — заметки для проверяющего

## Что относится к решению студента

Реализация находится только в пакете `company.vk.edu.distrib.compute.jokeryga`:

- `JokerygaKVService.java` — HTTP API на `com.sun.net.httpserver.HttpServer`
- `InMemoryDao.java` — in-memory `Dao<byte[]>`
- `JokerygaKVServiceFactory.java` — фабрика для тестов

Регистрация в интеграционных тестах: класс `JokerygaKVServiceFactory` добавлен в `Set` внутри [`KVServiceFactoryArgumentsProvider.java`](src/integrationTest/java/company/vk/edu/distrib/compute/KVServiceFactoryArgumentsProvider.java) (как требует задание).

## Как запустить проверку локально

1. **JDK 25** — в проекте задан Java toolchain (`languageVersion = 25` в `build.gradle.kts`). Без JDK 25 Gradle не соберёт проект (или подтянет его через Foojay, если настроено в `settings.gradle.kts`).
2. Из корня репозитория:

   ```bash
   ./gradlew clean build
   ```

   Эквивалент по смыслу с README курса: `./gradlew check` (включает тесты и проверки стиля).

3. **IDE:** при запуске тестов или `Server` из IDE указать JVM-опцию **`-Xmx128m`** (как в README шаблона).

4. Запуск сервера приложения:

   ```bash
   ./gradlew run
   ```

## Почему красный CI не обязан означать ошибку в `jokeryga`

Задача `integrationTest` выполняет **одни и те же** сценарии (`SingleNodeTest`, `StartStopTest`, `ShardingTest` и т.д.) для **каждой** фабрики из `KVServiceFactoryArgumentsProvider` — там перечислены решения разных участников репозитория.

Если в логе GitHub Actions падает **один** тест, смотрите **имя параметра / фабрики** в строке с `FAILED` или в отчёте JUnit: если там **не** `JokerygaKVServiceFactory`, падение относится к **другой** реализации, а не к пакету `jokeryga`.

Быстрая ориентировка по логу (локально или в CI):

```bash
./gradlew clean integrationTest 2>&1 | tee /tmp/it.log
grep -i jokeryga /tmp/it.log
grep FAILED /tmp/it.log
```

Если все строки `FAILED` относятся к другим фабрикам, а для `Jokeryga` в логе только `PASSED`, решение студента проходит интеграционные тесты; общий статус `build` при этом может быть красным из‑за чужого пакета или нестабильного теста.

---

*Файл добавлен автором PR для ускорения ревью; при желании его можно удалить после проверки.*
