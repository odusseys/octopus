package scala.octopus

import java.util.concurrent
import java.util.concurrent.{Future, Callable, Executors}

import org.apache.spark.SparkContext

import scala.collection.{GenSeq, GenSeqLike, TraversableLike}
import scala.collection.generic.CanBuildFrom


/**
 * The context in which any datasets and jobs will be executed. There can be at most one OctopusContext
 * per spark context in an application. DataSets which are created are tied to this context.
 *
 * Created by umizrahi on 25/02/2016.
 */
class OctopusContext private(sc: SparkContext) {

  def getSparkContext = sc

  def deploy[T](data: Iterable[T]): DataSet[T] = new DeployedDataSet(data, this)

  def textFile(file: java.io.File): DataSet[String] = new TextDataSet(file)(this)

  private val jobExecutor = Executors.newCachedThreadPool()

  // private val jobExecutor = Executors.newSingleThreadExecutor()

  def runJobs[S, Repr[A] <: GenSeqLike[A, Repr[A]], That, That2]
  (jobs: Repr[() => S])
  (implicit bf: CanBuildFrom[Repr[Iterable[Int] => S], S, That],
   bf2: CanBuildFrom[Repr[() => S], Iterable[Int] => S, Repr[Iterable[Int] => S]]
    )
  : That = {
    val dummy = deploy(Iterable(1))
    val mappedJobs = jobs.map { job => (i: Iterable[Int]) => job() }
    runJobsOnDataSet(dummy, mappedJobs)
  }

  def runJobsOnDataSet[T, S, Repr[A] <: GenSeqLike[A, Repr[A]], That]
  (data: DataSet[T], jobs: Repr[Iterable[T] => S])
  (implicit bf: CanBuildFrom[Repr[Iterable[T] => S], S, That]): That = {
    submitJobsOnDataSet(data, jobs).get()
  }

  def submitJobs[S, Repr[A] <: GenSeqLike[A, Repr[A]], That]
  (jobs: Repr[() => S])
  (implicit bf: CanBuildFrom[Repr[Iterable[Int] => S], S, That],
   bf2: CanBuildFrom[Repr[() => S], Iterable[Int] => S, Repr[Iterable[Int] => S]])
  : Future[That] = {
    val dummy = deploy(Iterable(1))
    val mappedJobs = jobs.map { job => (i: Iterable[Int]) => job() }
    submitJobsOnDataSet(dummy, mappedJobs)
  }

  def submitJobsOnDataSet[T, S, Repr[A] <: GenSeqLike[A, Repr[A]], That]
  (data: DataSet[T], jobs: Repr[Iterable[T] => S])
  (implicit bf: CanBuildFrom[Repr[Iterable[T] => S], S, That]): Future[That] = {
    if (jobs.isEmpty) {
      jobExecutor.submit(new Callable[That] {
        override def call(): That = jobs.map(_.asInstanceOf[S])
      })
    } else {
      jobExecutor.submit(new Callable[That] {
        override def call(): That = {
          implicit val cachedIds = cachedRegister.getRegistrationStatuses
          val nJobs = jobs.size
          val bcData = getSparkContext.broadcast(data)
          val jobTasks = jobs.toArray.map(j => new DeployedTask(j))
          val tempResults = getSparkContext.parallelize(1 to nJobs, nJobs)
            .mapPartitionsWithIndex { case (i, dat) =>
            Iterator((i, jobTasks(i).getTask(bcData.value.getData)))
          }
            .collect()
            .sortBy(_._1).map { case (i, res) => (jobs(i), res) }.toMap
          jobs.map(tempResults.apply)
        }
      })
    }
  }

  private[octopus] val dataSetRegister = new Register
  private[octopus] val cachedRegister = new Register

  private[octopus] def register(dataSet: DataSet[_]) = dataSetRegister.synchronized {
    dataSetRegister.register(dataSet)
  }

  private[octopus] def registerToCache(dataSet: DataSet[_]) = cachedRegister.synchronized {
    val id = cachedRegister.register(dataSet.id, dataSet)
    id
  }

  private[octopus] def unregisterFromCache(dataSet: DataSet[_]) = cachedRegister.synchronized {
    val id = cachedRegister.unregister(dataSet)
    id
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

}


