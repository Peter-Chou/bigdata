# Spark Playground

## compile

``` bash
# in bigdata/flink
mvn scala:compile
```


## examples

``` bash
# in bigdata/flink
mvn scala:run -DmainClass=peterchou.flink.examples.batch.wordCount.BatchWordCount
mvn scala:run -DmainClass=peterchou.flink.examples.stream.wordCount.StreamWordCount -DaddArgs="--host|localhost|--port|9999"
```

## package

``` bash
mvn package assembly:single
```

## submit

``` bash
# in project_root/flink folder
/Path/to/fLink/bin/flink run -c peterchou.flink.examples.stream.wordCount.StreamWordCount -p 1 ./target/flink-1.0-SNAPSHOT-jar-with-dependencies.jar --host localhost --port 9999
```
