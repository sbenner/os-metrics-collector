Processor Temperature Metrics sender/saver
----------------------------------------
**Requirements**

* java8 (JAVA_HOME env var set, java executable in the path of the current user)
* maven (mvn executable in the path of the current user)

**Build and Run**
------------
```
$ git clone https://github.com/sbenner/os-metrics-collector.git
$ cd os-metrics-collector
$ ./build_and_run.sh
$ tail -f producer/target/nohup.out
$ tail -f consumer/target/nohup.out
```
This will basically run the producer and consumer

**Attributes/Articles used**

https://stackoverflow.com/questions/22446478/extension-exists-but-uuid-generate-v4-fails
https://examples.javacodegeeks.com/core-java/mockito/mockito-mock-database-connection-example/
https://www.codota.com/code/java/methods/org.apache.kafka.clients.consumer.MockConsumer/seek
https://dzone.com/articles/using-powermock-mock-static

```
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```