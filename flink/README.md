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
