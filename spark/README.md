# Spark Playground

``` bash
mvn scala:compile
```

## examples

``` bash
mvn scala:run -DmainClass=peterchou.spark.examples.wordcount.WordCount
mvn scala:run -DmainClass=peterchou.spark.examples.agentlog.AgentLog
mvn scala:run -DmainClass=peterchou.spark.examples.topk.Topk
```

## transformations

``` bash
# sample
mvn scala:run -DmainClass=peterchou.spark.rdd.sampler.Sampler
# sortby
mvn scala:run -DmainClass=peterchou.spark.rdd.sortby.SortBy
# reduceByKey
mvn scala:run -DmainClass=peterchou.spark.rdd.reduceByKey.ReduceByKey
# aggregateByKey
mvn scala:run -DmainClass=peterchou.spark.rdd.aggregateByKey.AggregateByKey
# join
mvn scala:run -DmainClass=peterchou.spark.rdd.join.Join
# left join
mvn scala:run -DmainClass=peterchou.spark.rdd.leftJoin.LeftJoin
# cogroup
mvn scala:run -DmainClass=peterchou.spark.rdd.cogroup.CoGroup
# persist
mvn scala:run -DmainClass=peterchou.spark.rdd.persist.Persist
# partitionBy
mvn scala:run -DmainClass=peterchou.spark.rdd.partitioner.Partitioner
```

``` bash
mvn scala:run -DmainClass=peterchou.spark.accumulator.longAccumulator.LongAccumulator
mvn scala:run -DmainClass=peterchou.spark.accumulator.customAccumulator.CustomAccumulator
mvn scala:run -DmainClass=peterchou.spark.broadcast.Broadcast
```

## sql

``` bash
mvn scala:run -DmainClass=peterchou.spark.sql.basic.SqlBasic
mvn scala:run -DmainClass=peterchou.spark.sql.udf.UDF
mvn scala:run -DmainClass=peterchou.spark.sql.udaf.UDAF
```

## streaming

``` bash
mvn scala:run -DmainClass=peterchou.spark.streaming.streamingWordCount.StreamingWordCount
mvn scala:run -DmainClass=peterchou.spark.streaming.customReceiver.CustomReceiver
```
