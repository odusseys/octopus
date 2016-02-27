package scala.octopus

/**
 * Created by umizrahi on 27/02/2016.
 */
class DeployedTask[T](task: T)(implicit cacheIds: List[Int]) {

  def getTask(): T = DataCache.synchronized {
    DataCache.clean(cacheIds)
    task
  }
}
