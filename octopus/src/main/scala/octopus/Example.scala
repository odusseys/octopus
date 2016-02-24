package main.scala.octopus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

//import scala.collection.immutable.{Iterable, List, Seq}

import scala.collection.mutable

/**
 * Created by umizrahi on 23/02/2016.
 */
object Example {

  def main(args: Array[String]) {

    import DataSet._

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

    jobs.map(j => j(toDeploy.map(_ + 1).filter(_ % 2 == 0))).zip(rsltsjob).foreach(println)

  }


}
