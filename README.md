# octopus

A framework for job distribution and data replication on large clusters of machines, based off Apache Spark, taking advantage of Spark's 
resilience and simple framework for task distribution. 

Octopus' abstraction for data replication is the DataSet class, which represents data replicated on all machines in the cluster. 
Unlike Spark RDD's, DataSets do not partition their data accross the cluster, but simply create a copy on each machine. 
DataSets can be acted on by lazy transformations (map, filter, flatMap, reduceByKey, groupBy, drop, take ...) which are not executed until the data is needed for jobs.

Octopus supports caching of the replicated data, for ulterior reuse. 
