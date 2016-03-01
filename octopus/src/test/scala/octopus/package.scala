package scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by umizrahi on 29/02/2016.
 */
package object octopus {

  import OctopusContext._

  val testContext = new SparkContext(new SparkConf().setAppName("octopus_tests").setMaster("local[*]")).getOctopusContext
}
