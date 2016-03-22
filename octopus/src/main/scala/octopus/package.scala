package scala

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by umizrahi on 16/03/2016.
 */
package object octopus {
  def innerJoin[T, S, U](first: Iterable[(T, S)], second: Iterable[(T, U)]) = {
    val mapper = new mutable.HashMap[T, ArrayBuffer[S]]
    first.foreach { case (t, s) =>
      mapper.get(t) match {
        case Some(buffer) => buffer.append(s)
        case None =>
          val buf = new ArrayBuffer[S]
          buf.append(s)
          mapper.put(t, buf)
      }
    }
    second.flatMap { case (t, u) => mapper.get(t) match {
      case None => List()
      case Some(buffer) => buffer.map(s => (t, (s, u)))
    }
    }
  }

  def leftOuterJoin[T, S, U](first: Iterable[(T, S)], second: Iterable[(T, U)]) = {
    val mapper = new mutable.HashMap[T, ArrayBuffer[U]]
    second.foreach { case (t, s) =>
      mapper.get(t) match {
        case Some(buffer) => buffer.append(s)
        case None =>
          val buf = new ArrayBuffer[U]
          buf.append(s)
          mapper.put(t, buf)
      }
    }
    first.flatMap { case (t, u) => mapper.get(t) match {
      case None => List((t, (u, None)))
      case Some(buffer) => buffer.map(s => (t, (u, Some(s))))
    }
    }
  }

}
