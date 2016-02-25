package scala.octopus.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.octopus.OctopusContext

//import scala.collection.immutable.{Iterable, List, Seq}

/**
 * Created by umizrahi on 23/02/2016.
 */
object Example {

  def main(args: Array[String]) {

    import OctopusContext._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("octopus")
    val sc = new SparkContext(conf)

    val nJobs = 100

    def job(i: Int)(data: Iterable[Int]) = {
      data.map(_ * i).sum
    }

    val jobs = (1 to nJobs).map(i => job(i) _).toSeq

    val u = Iterable(1, 2, 3)
    val v = u.view.map(i => i.toDouble).force

    val toDeploy = 1 to 1000 toList

    val dat = sc.getOctopusContext.deploy(toDeploy)
      .map(i => i + 100)
      .filter(_ % 2 == 0)
    val rsltsjob = dat.execute(jobs)

    println("after ops")
    dat.fetch().foreach(println)

  }


}
