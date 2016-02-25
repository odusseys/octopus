package main.scala.octopus

import org.apache.spark.SparkContext

import scala.io.Source

/**
 * Created by umizrahi on 24/02/2016.
 */
sealed trait DataSet[T] extends Serializable {
  private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S]

  private[octopus] def getData: Iterable[T]

  def getContext: SparkContext

  def fetch() = getData

  /** Executes all the jobs provided by sending them to the executors for parallelization. */
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


  def zip[S](other: DataSet[S]): DataSet[(T, S)] = new ZippedDataSet(this, other)

  def collect[S](f: PartialFunction[T, S]) = transform(new Collect(f))

  def drop(n: Int) = transform(new Drop(n))

  def take(n: Int) = transform(new Take(n))

  def slice(from: Int, to: Int) = transform(new Slice(from, to))

  def zipWithIndex = transform(new ZipWithIndex)

}

class DeployedDataSet[T](data: Iterable[T])(@transient sc: SparkContext) extends DataSet[T] {
  println("SIZE :::: " + data.size)

  override private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S] =
    new TransformedDataSet(this, transformation)

  override def getData = data

  override def getContext = sc
}

class TransformedDataSet[T, S](origin: DataSet[T], transformation: Transformation[T, S]) extends DataSet[S] {
  override private[octopus] def transform[U](transformation: Transformation[S, U]): DataSet[U] =
    new TransformedDataSet(origin, this.transformation.andThen(transformation))

  override def getData: Iterable[S] =
    transformation.transform(origin.getData.view).force

  override def getContext = origin.getContext

}

/*Probably not a good solution at all, may want to import at creation and impose that data is on driver considering that the
* source of file may not allow concurrent requests at all. */
class TextDataSet(file: java.io.File)(@transient sc: SparkContext) extends DataSet[String] {
  override private[octopus] def transform[S](transformation: Transformation[String, S]): DataSet[S] = new TransformedDataSet(this, transformation)

  override private[octopus] def getData: Iterable[String] = Source.fromFile(file).getLines().toIterable

  override def getContext: SparkContext = sc
}

class ZippedDataSet[T, S](first: DataSet[T], second: DataSet[S]) extends DataSet[(T, S)] {
  require(first.getContext == second.getContext, "Cannot zip two datasets with different Spark contexts !")

  override private[octopus] def transform[U](transformation: Transformation[(T, S), U]): DataSet[U] =
    new TransformedDataSet(this, transformation)

  override private[octopus] def getData: Iterable[(T, S)] = first.getData.zip(second.getData)

  override def getContext: SparkContext = first.getContext
}

object DataSet {

  implicit class KeyOperations[K, V](data: DataSet[(K, V)]) {
    def reduceByKey(f: (V, V) => V) = data.transform(new ReduceByKey(f))

    def groupByKey = data.transform(new GroupByKey)

    def filterKeys(f: K => Boolean) = data.transform(new FilterKeys(f))

    def mapValues[U](f: V => U) = data.transform(new MapValues(f))

  }

}
