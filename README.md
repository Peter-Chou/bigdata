# Spark Playground

``` bash
mvn scala:compile
```

# examples

``` bash
mvn scala:run -DmainClass=peter.playground.wordcount.WordCount
```

## transformations

``` bash
mvn scala:run -DmainClass=peter.playground.transformations.sampler.Sampler
mvn scala:run -DmainClass=peter.playground.transformations.reduceByKey.ReduceByKey
mvn scala:run -DmainClass=peter.playground.transformations.aggregateByKey.AggregateByKey
```
