package scala.octopus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by umizrahi on 29/02/2016.
 */
class DataSetTest extends FunSuite {

  /* Tests for deployed datasets*/
  val dat = 1 to 10000 toList
  val deployed = testContext.deploy(dat)

  test("Transformations should behave as expected") {
    assert(deployed.map(_ + 1).fetch().zip(dat.map(_ + 1))
      .forall { case (u, v) => u == v })
    assert(deployed.filter(_ % 2 == 0).fetch().zip(dat.filter(_ % 2 == 0))
      .forall { case (u, v) => u == v })
    assert(deployed.flatMap(_ => List(1, 2)).fetch().zip(dat.flatMap(_ => List(1, 2)))
      .forall { case (u, v) => u == v })
    assert(deployed.drop(5).fetch().zip(dat.drop(5))
      .forall { case (u, v) => u == v })
    assert(deployed.take(5).fetch().zip(dat.take(5))
      .forall { case (u, v) => u == v })
    assert(deployed.first() == dat.head)
    assert(deployed.slice(5, 100).fetch().zip(dat.slice(5, 100))
      .forall { case (u, v) => u == v })
    assert(deployed.zipWithIndex.fetch().zip(dat.zipWithIndex)
      .forall { case (u, v) => u == v })
    assert(deployed.zip(deployed.map(i => i + 5)).fetch().zip(dat.zip(dat.map(i => i + 5)))
      .forall { case (u, v) => u == v })
  }

  test("Caching should speedup construction") {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val slowToBuild = deployed.map(i => {
      var t = 0
      (1 to 100000) foreach { _ => t = t + 1 }
      i + t
    })

    val t0 = System.currentTimeMillis()
    val slowRslts = (1 to 10) map { _ =>
      slowToBuild.execute(1 to 8 map { _ =>
        (it: Iterable[Int]) => it.sum
      })
    }
    val t1 = System.currentTimeMillis()
    val cached = slowToBuild.cache()
    val fastResults = (1 to 10) map { _ =>
      cached.execute(1 to 8 map { _ =>
        (it: Iterable[Int]) => it.sum
      })
    }
    val t2 = System.currentTimeMillis()
    assert((t2 - t1) < 0.5 * (t1 - t0))


    assert(slowRslts.flatten.zip(fastResults.flatten).forall { case (u, v) => u == v })

  }

}
