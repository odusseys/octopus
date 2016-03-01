package scala.octopus

import org.scalatest.FunSuite

/**
 * Created by umizrahi on 24/02/2016.
 */
class TransformationsTest extends FunSuite {

  def empty = Iterable[Int]().view

  def emptyKeyed = Iterable[(Int, Int)]().view

  def dat = Iterable(1, 2, 3, 4, 5).view

  def datKeyed = Iterable((1, 1), (1, 2), (1, 3), (2, 4), (2, 5)).view

  val mapper = (i: Int) => i + 1
  val filter = (i: Int) => i % 2 == 0
  val flatMapper = (i: Int) => Iterable(i, i)
  val grouper = (i: Int) => i % 2
  val reducer = (i: Int, j: Int) => i + j
  val partial: PartialFunction[Int, Int] = {
    case i: Int if i % 2 == 0 => i
  }

  /* tests for empty views*/
  test("Mapping an empty view should return an empty view") {
    assert(new MapTransformation(mapper).transform(empty).isEmpty)
  }

  test("Filtering an empty view should return an empty view") {
    assert(new Filter(filter).transform(empty).isEmpty)
  }

  test("Flatmapping an empty view should return an empty view") {
    assert(new FlatMap(flatMapper).transform(empty).isEmpty)
  }

  test("Grouping an empty view should return an empty view") {
    assert(new GroupBy(grouper).transform(empty).isEmpty)
  }

  test("Reducing an empty view should return an empty view") {
    assert(new ReduceByKey(reducer).transform(emptyKeyed).isEmpty)
  }

  test("Grouping an empty view by key should return an empty view") {
    assert(new GroupByKey().transform(emptyKeyed).isEmpty)
  }

  test("Filtering keys of an empty view should return an empty view") {
    assert(new FilterKeys(filter).transform(emptyKeyed).isEmpty)
  }

  test("Mapping values of an empty view should return an empty view") {
    assert(new MapValues(mapper).transform(emptyKeyed).isEmpty)
  }

  test("Collecting an empty view should return an empty view") {
    assert(new Collect(partial).transform(empty).isEmpty)
  }

  test("Taking from an empty view should return and empty view") {
    assert(new Take(1).transform(empty).isEmpty)
  }

  test("Dropping from an empty view should return and empty view") {
    assert(new Drop(1).transform(empty).isEmpty)
  }

  test("Slicing an empty view should return and empty view") {
    assert(new Slice(1, 3).transform(empty).isEmpty)
  }

  /* tests for synthetic data, non-keyed*/
  test("Map should act like map") {
    assert(new MapTransformation(mapper).transform(dat).force.zip(dat.force.map(mapper)).forall { case (i, j) => i == j })
  }

  test("Filter should act like filter") {
    assert(new Filter(filter).transform(dat).force.zip(dat.force.filter(filter)).forall { case (u, v) => u == v })
  }

  test("FlatMap should act like flatMap") {
    assert(new FlatMap(flatMapper).transform(dat).force.zip(dat.force.flatMap(flatMapper)).forall { case (i, j) => i == j })
  }

  test("GroupBy should act like GroupBy") {
    assert(new GroupBy(grouper).transform(dat).force.zip(dat.force.groupBy(grouper))
      .forall { case ((a, alist), (b, blist)) => alist.zip(blist).forall { case (u, v) => u == v } })
  }

  test("Collect should act like collect") {
    assert(new Collect(partial).transform(dat).force.zip(dat.collect(partial).force).forall { case (i, j) => i == j })
  }

  test("Take should act like take") {
    assert(new Take(2).transform(dat).force.zip(dat.take(2).force).forall { case (i, j) => i == j })
  }

  test("Drop should act like drop") {
    assert(new Drop(2).transform(dat).force.zip(dat.drop(2).force).forall { case (i, j) => i == j })
  }

  test("Slice should act like slice") {
    assert(new Slice(2, 4).transform(dat).force.zip(dat.slice(2, 4).force).forall { case (i, j) => i == j })
  }

  test("GroupByKey should group values by their associated keys") {
    val grouped = new GroupByKey().transform(datKeyed).force.mapValues(_.force).toList.sortBy(_._1)
    val expected = datKeyed.force.groupBy(_._1).mapValues(_.map(_._2)).toList.sortBy(_._1)
    grouped.zip(expected).foreach { case ((i, iti), (j, itj)) =>
      assert(i == j)
      assert(iti.zip(itj).forall { case (a, b) => a == b })
    }
  }

  test("ReduceByKey should reduce values by their associated keys") {
    val reduced = new ReduceByKey(reducer).transform(datKeyed).force.toList.sortBy(_._1)
    val expected = datKeyed.force.groupBy(_._1).mapValues(_.map(_._2).reduce(reducer)).toList.sortBy(_._1)
    reduced.zip(expected).foreach { case (u, v) => assert(u == v) }
  }

  test("Filtering keys should work like filterKeys") {
    val filtered = new FilterKeys(filter).transform(datKeyed).force.toList.sortBy(_._1)
    val expected = datKeyed.toMap.filterKeys(filter).toList.sortBy(_._1)
    filtered.zip(expected).forall { case (i, j) => i == j }
  }

  test("Mapping values should work like mapValues") {
    val mapped = new MapValues(mapper).transform(datKeyed).force.toList.sortBy(_._1)
    val expected = datKeyed.toMap.mapValues(mapper).toList.sortBy(_._1)
    mapped.zip(expected).forall { case (i, j) => i == j }
  }


}
