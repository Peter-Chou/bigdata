# Spark Playground

``` bash
mvn scala:compile
```

## examples

``` bash
mvn scala:run -DmainClass=peter.playground.examples.wordcount.WordCount
mvn scala:run -DmainClass=peter.playground.examples.agentlog.AgentLog
mvn scala:run -DmainClass=peter.playground.examples.topk.Topk
```

## transformations

``` bash
# sample
mvn scala:run -DmainClass=peter.playground.rdd.sampler.Sampler
# sortby
mvn scala:run -DmainClass=peter.playground.rdd.sortby.SortBy
# reduceByKey
mvn scala:run -DmainClass=peter.playground.rdd.reduceByKey.ReduceByKey
# aggregateByKey
mvn scala:run -DmainClass=peter.playground.rdd.aggregateByKey.AggregateByKey
# join
mvn scala:run -DmainClass=peter.playground.rdd.join.Join
# left join
mvn scala:run -DmainClass=peter.playground.rdd.leftJoin.LeftJoin
# cogroup
mvn scala:run -DmainClass=peter.playground.rdd.cogroup.CoGroup
# persist
mvn scala:run -DmainClass=peter.playground.rdd.persist.Persist
# partitionBy
mvn scala:run -DmainClass=peter.playground.rdd.partitioner.Partitioner
```

``` bash
mvn scala:run -DmainClass=peter.playground.accumulator.longAccumulator.LongAccumulator
mvn scala:run -DmainClass=peter.playground.accumulator.customAccumulator.CustomAccumulator
mvn scala:run -DmainClass=peter.playground.broadcast.Broadcast
```

## sql

``` bash
mvn scala:run -DmainClass=peter.playground.sql.basic.SqlBasic
mvn scala:run -DmainClass=peter.playground.sql.udf.UDF
mvn scala:run -DmainClass=peter.playground.sql.udaf.UDAF
```

## streaming

``` bash
mvn scala:run -DmainClass=peter.playground.streaming.streamingWordCount.StreamingWordCount
```
