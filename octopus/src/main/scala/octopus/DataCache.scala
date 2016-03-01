package scala.octopus

import scala.collection.mutable

/**
 * Created by umizrahi on 27/02/2016.
 */
private[octopus] object DataCache {

  private val cache = new mutable.HashMap[Int, Iterable[_]]

  def put(id: Int, data: Iterable[_]) = synchronized {
    cache.put(id, data)
  }

  def get(id: Int) = synchronized {
    cache.get(id)
  }

  def active() = synchronized {
    cache.keys.toList
  }

  def removeAll(ids: Iterable[Int]) = synchronized {
    ids foreach cache.remove
  }

}
