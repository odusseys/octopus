package scala.octopus

import org.apache.spark.SparkContext

import scala.collection.mutable


/**
 * The context in which any datasets and jobs will be executed. There can be at most one OctopusContext
 * per spark context in an application. DataSets which are created are tied to this context.
 *
 * Created by umizrahi on 25/02/2016.
 */
class OctopusContext private(sc: SparkContext) {

  import scala.octopus.OctopusContext._

  def getSparkContext = sc

  def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data, this)

  def textFile(file: java.io.File): DataSet[String] = new TextDataSet(file)(this)

  def executeJobs[T](jobs: Seq[() => T]) = {
    val dummy = deploy(Iterable(1))
    val mappedJobs = jobs.map { job => (i: Iterable[Int]) => job() }
    runJobsOnDataSet(dummy, mappedJobs)
  }

  def runJobsOnDataSet[T, S](data: DataSet[T], jobs: Seq[Iterable[T] => S]) = {
    implicit val cachedIds = cachedRegister.getIds
    val nJobs = jobs.length
    val bcData = getSparkContext.broadcast(data)
    val jobTasks = jobs.map(j => new DeployedTask[Iterable[T] => S](j))
    getSparkContext.parallelize(1 to nJobs, nJobs)
      .mapPartitionsWithIndex { case (i, dat) =>
      Iterator((i, jobTasks(i).getTask()(bcData.value.getData)))
    }
      .collect()
      .sortBy(_._1).map(_._2).toSeq
  }

  private val dataSetRegister = new Register
  private val cachedRegister = new Register

  private[octopus] def register(dataSet: DataSet[_]) = dataSetRegister.synchronized {
    dataSetRegister.register(dataSet)
  }

  private[octopus] def registerToCache(dataSet: DataSet[_]) = cachedRegister.synchronized {
    cachedRegister.register(dataSet)
  }

  private[octopus] def unregisterFromCache(dataSet: DataSet[_]) = cachedRegister.synchronized {
    cachedRegister.unregister(dataSet)
  }


}

object OctopusContext {

  private class Register {
    private val dataToId = new mutable.HashMap[DataSet[_], Int]
    private val idToData = new mutable.HashMap[Int, DataSet[_]]

    def register(data: DataSet[_]) = synchronized {
      if (dataToId.contains(data)) throw new IllegalStateException("Cannot register dataset more than once !")
      val id = dataToId.size
      dataToId.put(data, id)
      idToData.put(id, data)
      id
    }

    def getId(data: DataSet[_]) = synchronized {
      dataToId.get(data) match {
        case None => throw new NoSuchElementException("DataSet has not been registered yet !")
        case Some(x) => x
      }
    }

    def getDataSet(id: Int) = synchronized {
      idToData.get(id) match {
        case None => throw new NoSuchElementException("No DataSet has been registered for this id !")
        case Some(x) => x
      }
    }

    def unregister(data: DataSet[_]) = synchronized {
      val id = dataToId.remove(data)
      id match {
        case None => throw new NoSuchElementException("This dataset was not cached !")
        case Some(i) =>
          idToData.remove(i)
          id
      }
    }

    def getIds = synchronized {
      idToData.keys.toList
    }

  }

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

  private val contextMapping = new ContextMapping()

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

  /** Implicit conversion to use octopus context methods on a spark context */
  implicit def sparkToOctopus(sc: SparkContext): OctopusContext = sc.getOctopusContext

}


