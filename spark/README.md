# Spark Playground

``` bash
mvn scala:compile
```

## examples

``` bash
mvn scala:run -DmainClass=peter.spark.examples.wordcount.WordCount
mvn scala:run -DmainClass=peter.spark.examples.agentlog.AgentLog
mvn scala:run -DmainClass=peter.spark.examples.topk.Topk
```

## transformations

``` bash
# sample
mvn scala:run -DmainClass=peter.spark.rdd.sampler.Sampler
# sortby
mvn scala:run -DmainClass=peter.spark.rdd.sortby.SortBy
# reduceByKey
mvn scala:run -DmainClass=peter.spark.rdd.reduceByKey.ReduceByKey
# aggregateByKey
mvn scala:run -DmainClass=peter.spark.rdd.aggregateByKey.AggregateByKey
# join
mvn scala:run -DmainClass=peter.spark.rdd.join.Join
# left join
mvn scala:run -DmainClass=peter.spark.rdd.leftJoin.LeftJoin
# cogroup
mvn scala:run -DmainClass=peter.spark.rdd.cogroup.CoGroup
# persist
mvn scala:run -DmainClass=peter.spark.rdd.persist.Persist
# partitionBy
mvn scala:run -DmainClass=peter.spark.rdd.partitioner.Partitioner
```

``` bash
mvn scala:run -DmainClass=peter.spark.accumulator.longAccumulator.LongAccumulator
mvn scala:run -DmainClass=peter.spark.accumulator.customAccumulator.CustomAccumulator
mvn scala:run -DmainClass=peter.spark.broadcast.Broadcast
```

## sql

``` bash
mvn scala:run -DmainClass=peter.spark.sql.basic.SqlBasic
mvn scala:run -DmainClass=peter.spark.sql.udf.UDF
mvn scala:run -DmainClass=peter.spark.sql.udaf.UDAF
```

## streaming

``` bash
mvn scala:run -DmainClass=peter.spark.streaming.streamingWordCount.StreamingWordCount
mvn scala:run -DmainClass=peter.spark.streaming.customReceiver.CustomReceiver
```
