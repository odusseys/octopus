package scala.octopus

import main.scala.octopus.{TextDataSet, DataSet, DeployedDataSet}
import org.apache.spark.SparkContext

/**
 * Created by umizrahi on 25/02/2016.
 */
class OctopusContext(sc: SparkContext) {
  def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data)

  def textFile(file: java.io.File): DataSet[String] = new TextDataSet(file)

}

object OctopusContext {

  implicit class Make(sc: SparkContext) {
    def getOctopusContext = new OctopusContext(sc)
  }

}


