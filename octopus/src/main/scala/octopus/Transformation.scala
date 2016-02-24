package main.scala.octopus

import scala.collection.mutable

/**
 * Created by umizrahi on 24/02/2016.
 */
sealed trait Transformation[T, S] extends Serializable {
  def transform(data: Iterable[T]): Iterable[S]

  def andThen[U](other: Transformation[S, U]) = new AndThen(this, other)
}

class AndThen[S, T, U](first: Transformation[S, T], second: Transformation[T, U]) extends Transformation[S, U] {
  override def transform(data: Iterable[S]): Iterable[U] = second.transform(first.transform(data))
}

class Map[T, S](f: T => S) extends Transformation[T, S] {
  override def transform(data: Iterable[T]): Iterable[S] = data.map(f)
}

class FlatMap[T, S](f: T => TraversableOnce[S]) extends Transformation[T, S] {
  override def transform(data: Iterable[T]): Iterable[S] = data.flatMap(f)
}

class Filter[T](f: T => Boolean) extends Transformation[T, T] {
  override def transform(data: Iterable[T]): Iterable[T] = data.filter(f)
}

class GroupBy[T, S](f: T => S) extends Transformation[T, (S, Iterable[T])] {
  override def transform(data: Iterable[T]) = data.groupBy(f)
}

class ReduceByKey[K, V](reducer: (V, V) => V) extends Transformation[(K, V), (K, V)] {
  override def transform(data: Iterable[(K, V)]) = {
    val mapper = new mutable.HashMap[K, V]
    data.foreach { case (k, v) =>
      mapper.get(k) match {
        case None => mapper.put(k, v)
        case Some(t) => mapper.put(k, reducer(t, v))
      }
    }
    mapper.toList
  }
}

class GroupByKey[K, V] extends Transformation[(K, V), (K, Iterable[V])] {
  override def transform(data: Iterable[(K, V)]) = data.groupBy(_._1).mapValues(_.map(_._2))
}
