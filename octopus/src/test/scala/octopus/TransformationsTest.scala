package scala.octopus

import main.scala.octopus._
import org.scalatest.FunSuite

import scala.collection.IterableView

/**
 * Created by umizrahi on 24/02/2016.
 */
class TransformationsTest extends FunSuite {

  def empty = Iterable[Int]().view

  val dat = Iterable(1, 2, 3, 4, 5).view
  val mapper = (i: Int) => i + 1
  val filter = (i: Int) => i % 2 == 0
  val flatMapper = (i: Int) => Iterable(1, 2)
  val reducer = (i: Int, j: Int) => i + j

  test("Mapping an empty view should return an empty view") {
    assert(new Map(mapper).transform(empty).isEmpty)
  }

  test("Filtering an empty view should return an empty view") {
    assert(new Filter(filter).transform(empty).isEmpty)
  }

  test("Flatmapping an empty view should return an empty view") {
    assert(new FlatMap(flatMapper).transform(empty).isEmpty)
  }

  test("Reducing an empty view should return an empty view") {
    assert(new GroupBy(mapper).transform(empty).isEmpty)
  }

  test("Grouping an empty view should return an empty view") {
    assert(new ReduceByKey(reducer).transform(empty).isEmpty)
  }
}
