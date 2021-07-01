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
