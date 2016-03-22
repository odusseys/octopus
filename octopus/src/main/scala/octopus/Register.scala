package scala.octopus

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable


/**
 * Created by umizrahi on 01/03/2016.
 */
private[octopus] class Register {

  import scala.octopus.Register._

  private val dataToId = new mutable.WeakHashMap[DataSet[_], Int]
  private val idToStatus = new mutable.HashMap[Int, RegistrationStatus]
  private val idGenerator = new AtomicInteger()

  def generateId() = synchronized {
    var id = idGenerator.incrementAndGet()
    while (idToStatus.contains(id)) {
      id = idGenerator.incrementAndGet()
    }
    id
  }

  def register(data: DataSet[_]) = synchronized {
    if (dataToId.contains(data)) throw new IllegalStateException("Cannot register dataset more than once !")
    val id = generateId()
    dataToId.put(data, id)
    idToStatus.put(id, Registered)
    id
  }

  def register(id: Int, data: DataSet[_]) = synchronized {
    if (dataToId.contains(data)) throw new IllegalStateException("Cannot register dataset more than once !")
    dataToId.put(data, id)
    idToStatus.put(id, Registered)
    id
  }

  def getId(data: DataSet[_]) = synchronized {
    dataToId.get(data) match {
      case None => throw new NoSuchElementException("DataSet has not been registered yet !")
      case Some(x) => x
    }
  }

  def unregister(data: DataSet[_]) = synchronized {
    dataToId.remove(data) match {
      case None => throw new NoSuchElementException("This dataset was not cached !")
      case Some(i) =>
        idToStatus.put(i, Unregistered)
        i
    }
  }

  private def updateStatuses() = synchronized {
    //dataToId.
    val stillActive = dataToId.values.toSet
    idToStatus.keys.withFilter(!stillActive.contains(_)).foreach { i => idToStatus.put(i, Unregistered) }
  }

  def getRegistrationStatuses = synchronized {
    updateStatuses()
    idToStatus.toMap
  }
}

object Register {

  sealed abstract class RegistrationStatus

  case object Registered extends RegistrationStatus

  case object Unregistered extends RegistrationStatus

}
