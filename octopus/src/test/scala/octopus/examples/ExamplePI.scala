package scala.octopus.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.octopus.OctopusContext

/**
 * Created by umizrahi on 25/02/2016.
 */
object ExamplePI {

  /* test estimation of PI through monte carlo, by using the OctopusContext.executeJobs method*/
  def main(args: Array[String]) {
    import OctopusContext._
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test_mc"))

    def job(n: Int)() = {
      var hits = 0.0
      (1 to n).foreach { _ =>
        val x = 2 * math.random - 1
        val y = 2 * math.random - 1
        if (x * x + y * y < 1) hits = hits + 1.0
      }
      (hits, n.toDouble)
    }

    val jobs = (1 to 10) map { _ => job(1000000) _ } toList
    val results = sc.getOctopusContext.executeJobs(jobs).toList
    val (hits,total) = results.foldLeft((0.0,0.0)) { case ((u, v), (up, vp)) => (u + up, v + vp) }
    println(s"PI is approximately ${4 * hits/total}")
  }

}
