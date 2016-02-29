package scala.octopus

import scala.collection.mutable

/**
 * Created by umizrahi on 27/02/2016.
 */
private[octopus] object DataCache {

  val cache = new mutable.HashMap[Int, Iterable[_]]

  def put(id: Int, data: Iterable[_]) = synchronized {
    cache.put(id, data)
  }

  def get(id: Int) = synchronized {
    cache.get(id)
  }

  def clean(activeIds: List[Int]) = synchronized {
    (cache.keySet -- activeIds) foreach cache.remove
  }


}
