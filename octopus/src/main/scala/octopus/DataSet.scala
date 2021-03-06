package scala.octopus

import java.util.concurrent.atomic.AtomicReference

import scala.io.Source

/**
 * Octopus' main abstraction. A DataSet represents data which may have transformations applied to it.
 * DataSets are not effectively computed until needed, ie. when jobs are executed on them,
 * at which point they will be broadcasted to the network any pending calculations will
 * be made on the respective workers.
 *
 * This allows to delegate calculations which may potentially
 * greatly increase the size of the data to the workers, rather than to send the whole dataset
 * across the network.
 * It also allows to make each worker pull a file and create a DataSet on it,
 * and then operate on it separately, thus never causing communication of the data on the network.
 *
 * Created by umizrahi on 24/02/2016.
 */
sealed trait DataSet[T] extends Serializable {

  /*Applies a Transformation object to this DataSet*/
  private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S]

  /*Get the data in this DataSet in iterable form. Should trigger transformation computation job. */
  private[octopus] def getData: Iterable[T]

  /*Register the dataset on the spark context*/
  private[octopus] val id: Int = getContext.register(this)

  /** Returns the Spark context object associated with this DataSet. This is always a valid context. */
  def getContext: OctopusContext

  /** Retrieve the data in this DataSet in iterable form. Any pending transformations will
    * be computed. */
  def fetch(): Iterable[T] = getData


  /** Cache the dataset in memory on all workers.
    * Note that the caching will not be instantly triggered by calling this method : instead,
    * the next time this dataset will be computed (by runnign jobs on it), it will be cached. */
  final def cache(): DataSet[T] = {
    val cachedData = getCachedDataSet
    getContext.registerToCache(cachedData)
    cachedData
  }

  private[octopus] def getCachedDataSet: DataSet[T]

  /** Unpersists the dataset if it was cached. If it was not cached, has no effect.
    * Note that this does not immediately unpersisty the data in local caches ; instead, the
    * caches will be cleared the next time a job is executed on the associated OctopusContext
    * (or any associated DataSet's) */
  def unpersist(): Unit

  /** Executes all the jobs provided by sending them to the executors for parallelization. */
  def execute[S](jobs: Seq[Iterable[T] => S]): Seq[S] = getContext.runJobsOnDataSet(this, jobs)

  /** Maps the DataSet lazily, by applying the given function to each element of the underlying collection. */
  def map[S](f: T => S) = transform(new MapTransformation(f))

  /** Filters the DataSet lazily, by applying the given predicate to each element of the underlying collection. */
  def filter(f: T => Boolean) = transform(new Filter(f))

  /** FlatMaps the DataSet lazily, by applying the given function to each element of the underlying collection
    * and flattening the result. */
  def flatMap[S](f: T => TraversableOnce[S]) = transform(new FlatMap(f))

  /** Lazily groups the data according the the given partitioning function, returning a key,value
    * DataSet where the keys are the grouping keys and the values are the elements corresponding to this key */
  def groupBy[S](f: T => S) = transform(new GroupBy(f))

  /** Zips this DataSet wit another lazily. If one is larger than the other, remaining elements
    * are ignored */
  def zip[S](other: DataSet[S]): DataSet[(T, S)] = new ZippedDataSet(this, other)

  /** Collects the elements in the DataSet lazily, by applying the given partial function. */
  def collect[S](f: PartialFunction[T, S]) = transform(new Collect(f))

  /** Drops the first n elements from this DataSet */
  def drop(n: Int) = transform(new Drop(n))

  /** Takes the first n elements from this DataSet */
  def take(n: Int) = transform(new Take(n))

  /** Drops the first "from" elements in this DataSet and takes the elements up to "to" */
  def slice(from: Int, to: Int) = transform(new Slice(from, to))

  /** Zips elements of this DataSet with their index (position, from 0 to size -1) in the dataset */
  def zipWithIndex = transform(new ZipWithIndex)

  /** Returns the number of elements in this DataSet.
    * This will cause all transformations applied to this dataset to be computed. */
  def size() = fetch().size

  /** Same as size, to unify with RDD API */
  def count() = size()

  /** Returns the first element in this DataSet
    * This will cause all transformations applied to this dataset to be computed. */
  def first() = fetch().head

}

private[octopus] sealed trait UncachedDataSet[T] extends DataSet[T] {
  override private[octopus] def getCachedDataSet: DataSet[T] = new CachedDataSet[T](this)

  override def unpersist(): Unit = {}
}

/* Implementation of DataSet which has concrete data attached to it. Only obtained through OctopusContext.deploy*/
private[octopus] class DeployedDataSet[T]
(data: Iterable[T], @transient oc: OctopusContext) extends UncachedDataSet[T] {

  override private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S] =
    new TransformedDataSet(this, transformation)

  override def getData = {
    data
  }

  override def getContext = oc
}

private[octopus] class TransformedDataSet[T, S](origin: DataSet[T], transformation: Transformation[T, S]) extends UncachedDataSet[S] {
  override private[octopus] def transform[U](transformation: Transformation[S, U]): DataSet[U] =
    new TransformedDataSet(origin, this.transformation.andThen(transformation))

  override def getData: Iterable[S] =
    transformation.transform(origin.getData.view).force

  override def getContext = origin.getContext

  override private[octopus] val id: Int = origin.id
}

/*Probably not a good solution at all, may want to import at creation and impose that data is on driver considering that the
* source of file may not allow concurrent requests at all. */
private[octopus] class TextDataSet(file: java.io.File)(@transient oc: OctopusContext) extends UncachedDataSet[String] {
  override private[octopus] def transform[S](transformation: Transformation[String, S]): DataSet[S] = new TransformedDataSet(this, transformation)

  override private[octopus] def getData: Iterable[String] = Source.fromFile(file).getLines().toIterable

  override def getContext: OctopusContext = oc

}

/*DataSet representing two zipped DataSet's. This allows to zip lazily.*/
private[octopus] class ZippedDataSet[T, S](first: DataSet[T], second: DataSet[S]) extends UncachedDataSet[(T, S)] {
  require(first.getContext == second.getContext, "Cannot zip two datasets with different Spark contexts !")

  override private[octopus] def transform[U](transformation: Transformation[(T, S), U]): DataSet[U] =
    new TransformedDataSet(this, transformation)

  override private[octopus] def getData: Iterable[(T, S)] = first.getData.zip(second.getData)

  override def getContext: OctopusContext = first.getContext
}

/*Cached datasets. Once it is computed it is 1) stored in the cache and 2) stored in the object (on workers).
* */
private[octopus] class CachedDataSet[T](origin: DataSet[T]) extends DataSet[T] {

  @transient var data = new AtomicReference[Iterable[T]]() //this layer of caching may not be necessary actually.

  /*Applies a Transformation object to this DataSet*/
  override private[octopus] def transform[S](transformation: Transformation[T, S]): DataSet[S] = new TransformedDataSet(this, transformation)

  /*Get the data in this DataSet in iterable form. Should trigger transformation computation job. */
  override private[octopus] def getData: Iterable[T] = {
    val get = data.get()
    if (get != null) {
      get
    } else {
      DataCache.synchronized {
        DataCache.get(id) match {
          case None =>
            val built = origin.getData
            DataCache.put(id, built)
            data.set(built)
            built
          case Some(built: Iterable[T]) =>
            data.set(built)
            built
          case _ => throw new IllegalStateException("Requestion cached data of the wrong type")
        }
      }
    }

  }

  /** Returns the Spark context object associated with this DataSet */
  override def getContext: OctopusContext = origin.getContext

  override private[octopus] def getCachedDataSet: DataSet[T] = this

  override def unpersist(): Unit = {
    getContext.unregisterFromCache(this)
  }
}

object DataSet {


  /** Provides additional transformations for key/value DataSets (note : could also implement
    * this through implicit conversion, but not necessary at the moment). */
  implicit class KeyValueOps[K, V](data: DataSet[(K, V)]) {

    /** Lazily reduces the data associated to each key by applying the provided reduction function.
      * For guaranteed results, this function should be commutative and associative. */
    def reduceByKey(f: (V, V) => V) = data.transform(new ReduceByKey(f))

    /** Lazily groups data in this DataSet according to their keys, much like groupBy, except the
      * value lists are only made of the values in this key/value DataSet */
    def groupByKey = data.transform(new GroupByKey)

    /** Lazily filters keys in this DataSet according to the given predicate. */
    def filterKeys(f: K => Boolean) = data.transform(new FilterKeys(f))

    /** Lazily maps values in this DataSet according to the given function */
    def mapValues[U](f: V => U) = data.transform(new MapValues(f))

  }

}
