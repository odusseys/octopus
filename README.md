# octopus

A framework for job distribution and data replication on large clusters of machines, based off Apache Spark, taking advantage of Spark's 
resilience and simple framework for task distribution. 

```
import OctopusContext._
val sparContext: SparkContext = ???
val oc = sparkContext.getOctopusContext()
```

Octopus' abstraction for data replication is the DataSet class, which represents data replicated on all machines in the cluster. 
Unlike Spark RDD's, DataSets do not partition their data accross the cluster, but simply create a copy on each machine. 
DataSets can be acted on by lazy transformations (map, filter, flatMap, reduceByKey, groupBy, drop, take ...) which are not executed until the data is needed for jobs.

```
val data: DataSet[Int] = oc.deploy(1 to 1000)
val transformed: DataSet[(Int,Int)] = data.map(_ + 1).filter(_ % 2 == 0)
    .flatMap(i => 1 to i)
    .map(i => (i, i + 1))
    .reduceByKey(_ * _) //transformations not computed yet - data not even deployed yet 

val jobs = (1 to 10).map(i => (it: Iterable[(Int,Int)]) => it.map(_._2 + i).sum).toSeq
val results: Seq[Int] = data.executeJobs(jobs)
```

Octopus supports caching of the replicated data, for ulterior reuse. 
```
val cached = data.cache()
cached.unpersist()
```

You can also launch jobs asynchronously : 
```
val results: Future[Seq[Int]] = data.submitJobs(jobs)
```
