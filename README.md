# Spark Playground

``` bash
mvn scala:compile
```

# examples

``` bash
mvn scala:run -DmainClass=peter.playground.examples.wordcount.WordCount
mvn scala:run -DmainClass=peter.playground.examples.agentlog.AgentLog
```

## transformations

``` bash
# sample
mvn scala:run -DmainClass=peter.playground.transformations.sampler.Sampler
# sortby
mvn scala:run -DmainClass=peter.playground.transformations.sortby.SortBy
# reduceByKey
mvn scala:run -DmainClass=peter.playground.transformations.reduceByKey.ReduceByKey
# aggregateByKey
mvn scala:run -DmainClass=peter.playground.transformations.aggregateByKey.AggregateByKey
# join
mvn scala:run -DmainClass=peter.playground.transformations.join.Join
# left join
mvn scala:run -DmainClass=peter.playground.transformations.leftJoin.LeftJoin
# cogroup
mvn scala:run -DmainClass=peter.playground.transformations.cogroup.CoGroup
```
