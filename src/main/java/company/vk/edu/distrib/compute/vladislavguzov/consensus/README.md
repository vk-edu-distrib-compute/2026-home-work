# Consensus — Bully Leader Election

## Тесты

> `JAVA_HOME` должен указывать на JDK 25.

```powershell
./gradlew test --tests "company.vk.edu.distrib.compute.vladislavguzov.consensus.*"
```

## Демо-запуск

```powershell
./gradlew classes -q

$GC = "$env:USERPROFILE\.gradle\caches\modules-2\files-2.1"
$JARS = (Get-ChildItem "$GC\ch.qos.logback", "$GC\org.slf4j" -Filter "*.jar" -Recurse).FullName -join ";"
$CP = "build\classes\java\main;build\resources\main;$JARS"
& "$env:JAVA_HOME\bin\java.exe" -Xmx128m -cp $CP company.vk.edu.distrib.compute.vladislavguzov.consensus.ConsensusMain
```

## Что происходит в демо

1. Запускается кластер из 5 узлов с включёнными случайными сбоями
2. Печатается начальное состояние — лидер node-4 (наибольший ID)
3. Лидер принудительно останавливается — система выбирает нового
4. Лидер восстанавливается — перехватывает лидерство обратно
