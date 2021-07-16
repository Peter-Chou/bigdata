# Kafka Playground

## compile

``` bash
# in bigdata/flink
mvn compile
```


## examples

``` bash
# in bigdata/kafka
mvn exec:java -Dexec.mainClass="peterchou.kafka.examples.baseProducer.BaseProducer"
mvn exec:java -Dexec.mainClass="peterchou.kafka.examples.baseConsumer.BaseConsumer"
mvn exec:java -Dexec.mainClass="peterchou.kafka.examples.asyncProducer.AsyncProducer"
```

## package

``` bash
mvn package
```
