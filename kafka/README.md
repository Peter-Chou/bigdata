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
```

## package

``` bash
mvn package
```
