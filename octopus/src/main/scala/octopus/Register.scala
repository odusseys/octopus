package scala.octopus

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable


/**
 * Created by umizrahi on 01/03/2016.
 */
class Register {

  import scala.octopus.Register._

  private val dataToId = new mutable.HashMap[DataSet[_], Int]
  private val idToData = new mutable.HashMap[Int, DataSet[_]]
  private val idToStatus = new mutable.HashMap[Int, RegistrationStatus]
  private val idGenerator = new AtomicInteger()

  def generateId() = synchronized {
    var id = idGenerator.incrementAndGet()
    while (idToData.contains(id)) {
      id = idGenerator.incrementAndGet()
    }
    id
  }

  def register(data: DataSet[_]) = synchronized {
    if (dataToId.contains(data)) throw new IllegalStateException("Cannot register dataset more than once !")
    val id = generateId()
    dataToId.put(data, id)
    idToData.put(id, data)
    idToStatus.put(id, Registered)
    id
  }

  def register(id: Int, data: DataSet[_]) = synchronized {
    if (dataToId.contains(data)) throw new IllegalStateException("Cannot register dataset more than once !")
    dataToId.put(data, id)
    idToData.put(id, data)
    idToStatus.put(id, Registered)
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
    dataToId.remove(data) match {
      case None => throw new NoSuchElementException("This dataset was not cached !")
      case Some(i) =>
        idToData.remove(i)
        idToStatus.put(i, Unregistered)
        i
    }
  }

  def getRegistrationStatuses = synchronized {
    idToStatus.toMap
  }

}

object Register {

  sealed abstract class RegistrationStatus

  case object Registered extends RegistrationStatus

  case object Unregistered extends RegistrationStatus

}
