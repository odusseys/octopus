package scala.octopus

import scala.octopus.Register._

/**
 * Created by umizrahi on 27/02/2016.
 */
private[octopus] class DeployedTask[T](task: T)(implicit cacheSatuses: Map[Int, RegistrationStatus]) extends Serializable {

  private def performDeployedActions() = {
    DataCache.removeAll(cacheSatuses.collect { case (u, stat) if stat == Unregistered => u })
  }

  def getTask: T = DataCache.synchronized {
    performDeployedActions()
    task
  }
}
