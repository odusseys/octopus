package main.scala.octopus

import org.apache.spark.SparkContext

/**
 * Created by umizrahi on 24/02/2016.
 */
sealed trait DataSet[T] extends Serializable {
  private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S]

  private[octopus] def getData: Iterable[T]

  def getContext: SparkContext

  def execute[S](jobs: Seq[Iterable[T] => S]): Seq[S] = {
    val nJobs = jobs.length
    val bcData = getContext.broadcast(this)
    getContext.parallelize(1 to nJobs, nJobs)
      .mapPartitionsWithIndex { case (i, dat) => Iterator((i, jobs(i)(bcData.value.getData))) }
      .collect()
      .sortBy(_._1).map(_._2).toList
  }

  def map[S](f: T => S) = transform(new Map(f))

  def filter(f: T => Boolean) = transform(new Filter(f))

  def flatMap[S](f: T => TraversableOnce[S]) = transform(new FlatMap(f))

  def groupBy[S](f: T => S) = transform(new GroupBy(f))

  def collect() = getData

}

class DeployedDataSet[T](data: Iterable[T])(@transient sc: SparkContext) extends DataSet[T] {
  println("SIZE :::: " + data.size)
  override private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S] =
    new TransformedDataSet(this, transformation)

  override def getData = data

  override def getContext = sc
}

class TransformedDataSet[T, S](origin: DeployedDataSet[T], transformation: Transformation[T, S]) extends DataSet[S] {
  override private[octopus] def transform[U](transformation: Transformation[S, U]): DataSet[U] = {
    println("TRANSFORMING with " + transformation.getClass)
    new TransformedDataSet(origin, this.transformation.andThen(transformation))
  }


  override def getData: Iterable[S] = {
    println("FETCHING DATA ! Transformation " + transformation.getClass)

    val dat = transformation.transform(origin.getData)
    println(dat.size + " " + origin.getData.size)
    dat
  }

  override def getContext = origin.getContext

}



object DataSet {

  implicit class KeyOperations[K, V](data: DataSet[(K, V)]) {
    def reduceByKey(f: (V, V) => V) = data.transform(new ReduceByKey(f))

    def groupByKey = data.transform(new GroupByKey)
  }

  implicit class Deployment(sc: SparkContext) {
    def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data)(sc)
  }

}
