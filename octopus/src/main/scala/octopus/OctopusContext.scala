package scala.octopus

import java.util.concurrent.atomic.AtomicReference

import main.scala.octopus.{TextDataSet, DataSet, DeployedDataSet}
import org.apache.spark.SparkContext

/**
 * Created by umizrahi on 25/02/2016.
 */
class OctopusContext private(sc: SparkContext) {
  def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data)(sc)

  def textFile(file: java.io.File): DataSet[String] = new TextDataSet(file)(sc)

  def executeJobs[T](jobs: List[() => T]) = {
    val dummy = deploy(Iterable(1))
    val mappedJobs = jobs.map { job => (i: Iterable[Int]) => job() }
    dummy.execute(mappedJobs)
  }

}

object OctopusContext {

  private class ContextMapping() {
    var sparkContext: SparkContext = null
    var octopusContext: OctopusContext = null

    def set(sc: SparkContext, oc: OctopusContext) = this.synchronized {
      require(sc != null)
      require(oc != null)
      if (sparkContext != null) throw new IllegalStateException("Only one octopus context may be active at a time")
      sparkContext = sc
      octopusContext = oc
    }

    def getOctopusContext = this.synchronized {
      octopusContext
    }

    def getSparkContext = this.synchronized {
      sparkContext
    }

  }

  private[octopus] val contextMapping = new ContextMapping()

  implicit class Make(sc: SparkContext) {
    def getOctopusContext = contextMapping.synchronized {
      var oc = contextMapping.getOctopusContext
      if (oc == null) {
        oc = new OctopusContext(sc)
        contextMapping.set(sc, oc)
      }
      oc
    }
  }

}


