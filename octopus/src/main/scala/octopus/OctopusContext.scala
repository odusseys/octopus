package scala.octopus

import main.scala.octopus.{TextDataSet, DataSet, DeployedDataSet}
import org.apache.spark.SparkContext

/**
 * Created by umizrahi on 25/02/2016.
 */
class OctopusContext(sc: SparkContext) {
  def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data)(sc)

  def textFile(file: java.io.File): DataSet[String] = new TextDataSet(file)(sc)

  def executeJobs[T](jobs: List[() => T]) = {
    val dummy = deploy(Iterable(1))
    val mappedJobs = jobs.map { job => (i: Iterable[Int]) => job() }
    dummy.execute(mappedJobs)
  }

}

object OctopusContext {

  implicit class Make(sc: SparkContext) {
    def getOctopusContext = new OctopusContext(sc)
  }

}


