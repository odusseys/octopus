package scala.octopus

/**
 * Might be used later if API turns out not to too functional
 * Created by umizrahi on 25/02/2016.
 */
trait Job[T] {
  def execute(): T
}

object Job {
  implicit def toLambda[T](job: Job[T]): () => T = () => job.execute()

  implicit def fromLambda[T](job: () => T): Job[T] = new Job[T] {
    override def execute() = job()
  }
}
