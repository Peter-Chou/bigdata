# Spark Playground

## compile

``` bash
# in bigdata/flink
mvn compile
```


## examples

``` bash
# in bigdata/flink
mvn exec:java -Dexec.mainClass="peterchou.flink.examples.batch.wordCount.BatchWordCount"
mvn exec:java -Dexec.mainClass="peterchou.flink.examples.stream.wordCount.StreamWordCount" \
-Dexec.args="--host localhost --port 7777"

mvn exec:java -Dexec.mainClass="peterchou.flink.stream.source.sourceFile.SourceFile"
mvn exec:java -Dexec.mainClass="peterchou.flink.stream.source.sourceKafka.SourceKafka"

mvn exec:java -Dexec.mainClass="peterchou.flink.stream.source.sourceCustom.SourceCustom"

mvn exec:java -Dexec.mainClass="peterchou.flink.stream.transform.base.BaseTransform"
mvn exec:java -Dexec.mainClass="peterchou.flink.stream.transform.rollingAggreagtion.RollingAggregation"
mvn exec:java -Dexec.mainClass="peterchou.flink.stream.transform.reduce.ReduceTransform"
mvn exec:java -Dexec.mainClass="peterchou.flink.stream.transform.richFunction.RichFunction"

mvn exec:java -Dexec.mainClass="peterchou.flink.stream.sink.kafkaSink.KafkaSink"

```

## package

``` bash
mvn package
```

## submit

``` bash
# in project_root/flink folder
/Path/to/fLink/bin/flink run -c peterchou.flink.examples.stream.wordCount.StreamWordCount -p 1 ./target/flink-1.0-SNAPSHOT-jar-with-dependencies.jar --host localhost --port 7777
```
