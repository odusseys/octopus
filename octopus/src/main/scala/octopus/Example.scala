package main.scala.octopus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

//import scala.collection.immutable.{Iterable, List, Seq}

import scala.collection.mutable

/**
 * Created by umizrahi on 23/02/2016.
 */
object Example {

  trait Transformation[T, S] extends Serializable {
    def transform(data: Iterable[T]): Iterable[S]

    def andThen[U](other: Transformation[S, U]) = new AndThen(this, other)
  }

  class AndThen[S, T, U](first: Transformation[S, T], second: Transformation[T, U]) extends Transformation[S, U] {
    override def transform(data: Iterable[S]): Iterable[U] = second.transform(first.transform(data))
  }

  class Map[T, S](f: T => S) extends Transformation[T, S] {
    override def transform(data: Iterable[T]): Iterable[S] = data.map(f)
  }

  class FlatMap[T, S](f: T => TraversableOnce[S]) extends Transformation[T, S] {
    override def transform(data: Iterable[T]): Iterable[S] = data.flatMap(f)
  }

  class Filter[T](f: T => Boolean) extends Transformation[T, T] {
    override def transform(data: Iterable[T]): Iterable[T] = data.filter(f)
  }

  class GroupBy[T, S](f: T => S) extends Transformation[T, (S, Iterable[T])] {
    override def transform(data: Iterable[T]) = data.groupBy(f)
  }

  class ReduceByKey[K, V](reducer: (V, V) => V) extends Transformation[(K, V), (K, V)] {
    override def transform(data: Iterable[(K, V)]) = {
      val mapper = new mutable.HashMap[K, V]
      data.foreach { case (k, v) =>
        mapper.get(k) match {
          case None => mapper.put(k, v)
          case Some(t) => mapper.put(k, reducer(t, v))
        }
      }
      mapper.toList
    }
  }

  class GroupByKey[K, V] extends Transformation[(K, V), (K, Iterable[V])] {
    override def transform(data: Iterable[(K, V)]) = data.groupBy(_._1).mapValues(_.map(_._2))
  }

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

  implicit class KeyOperations[K, V](data: DataSet[(K, V)]) {
    def reduceByKey(f: (V, V) => V) = data.transform(new ReduceByKey(f))

    def groupByKey = data.transform(new GroupByKey)
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

  implicit class Deployment(sc: SparkContext) {
    def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data)(sc)
  }


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("octopus")
    val sc = new SparkContext(conf)
    val partitions = 100
    val rdd = sc.parallelize(1 to partitions, partitions)
    rdd.collect().foreach(println)

    val data = (1 to 1000).toList
    val bcData = sc.broadcast(data)

    def job(i: Int, data: List[Int]) = {
      data.map(_ * i).sum
    }

    val jobs = (1 to partitions).map(i => (list: Iterable[Int]) => job(i, data)).toSeq

    val rslts = rdd.mapPartitionsWithIndex { case (i, useless) =>
      Iterator(jobs(i)(bcData.value))
    }.collect()
   // rslts.foreach(println)

    println("with my method")

    val toDeploy = 1 to 1000 toList

    val dat = sc.deploy(toDeploy)
      .map(i => i + 100)
      //.map(i => i - 1)
      .filter(_ % 2 == 0)
      //.flatMap(i => List(i, i + 1))
    val rsltsjob = dat.execute(jobs)

    println("after ops")
    dat.collect().foreach(println)

    println("comparson")

    jobs.map(j => j(toDeploy.map(_ +1).filter(_%2==0))).zip(rsltsjob).foreach(println)

  }


}
