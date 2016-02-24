package main.scala.octopus

import scala.collection.{IterableView, mutable}

/**
 * Created by umizrahi on 24/02/2016.
 */
sealed trait Transformation[T, S] extends Serializable {
  def transform(data: IterableView[T, Iterable[_]]): IterableView[S, Iterable[_]]

  def andThen[U](other: Transformation[S, U]) = new AndThen(this, other)
}

class AndThen[S, T, U](first: Transformation[S, T], second: Transformation[T, U]) extends Transformation[S, U] {
  override def transform(data: IterableView[S, Iterable[_]]) = second.transform(first.transform(data))
}

class Map[T, S](f: T => S) extends Transformation[T, S] {
  override def transform(data: IterableView[T, Iterable[_]]) = data.map(f)
}

class FlatMap[T, S](f: T => TraversableOnce[S]) extends Transformation[T, S] {
  override def transform(data: IterableView[T, Iterable[_]]) = data.flatMap(f)
}

class Filter[T](f: T => Boolean) extends Transformation[T, T] {
  override def transform(data: IterableView[T, Iterable[_]]) = data.filter(f)
}

class GroupBy[T, S](f: T => S) extends Transformation[T, (S, Iterable[T])] {
  override def transform(data: IterableView[T, Iterable[_]]) = data.groupBy(f).view
}

class ReduceByKey[K, V](reducer: (V, V) => V) extends Transformation[(K, V), (K, V)] {
  override def transform(data: IterableView[(K, V), Iterable[_]]) = {
    val mapper = new mutable.HashMap[K, V]
    data.foreach { case (k, v) =>
      mapper.get(k) match {
        case None => mapper.put(k, v)
        case Some(t) => mapper.put(k, reducer(t, v))
      }
    }
    mapper.toList.view
  }
}

class GroupByKey[K, V] extends Transformation[(K, V), (K, Iterable[V])] {
  override def transform(data: IterableView[(K, V), Iterable[_]]) = data.groupBy(_._1).mapValues(_.map(_._2)).view
}


trait LazyTransformation[T, S] extends Serializable {
  def transform(data: IterableView[T, Iterable[_]]): IterableView[S, Iterable[_]]
}

class LazyMap[T, S](f: T => S) extends LazyTransformation[T, S] {
  override def transform(data: IterableView[T, Iterable[_]]) = data.map(f)
}

object testy {
  def main(args: Array[String]) {
    val t = new LazyMap((i: Int) => i + 2.0)
    println(t.transform(Iterable(1, 2, 3, 4, 5).view).force)
  }
}
