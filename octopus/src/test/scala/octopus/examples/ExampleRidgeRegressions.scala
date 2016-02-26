package scala.octopus.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Example which shows how to distribute grid search of the shrinkage parameter for Ridge Regression
 * Created by umizrahi on 25/02/2016.
 */
object ExampleRidgeRegressions {

  case class LabeledPoint(label: Double, features: Array[Double])

  class RidgeRegression(data: Iterable[LabeledPoint], shrinkage: Double) extends Serializable {
    val learningRate = 0.01
    val nEpochs = 10
    var baseline = 0.0
    val coefficients = Array.fill(data.head.features.length)(0.0)

    train()

    def predict(line: LabeledPoint) = baseline +
      coefficients.zip(line.features).map { case (u, v) => u * v }.sum

    def train() = {
      (1 to nEpochs) foreach { _ =>
        data.foreach { line =>
          val delta = -learningRate * (line.label - predict(line))
          baseline = baseline - delta
          coefficients.indices foreach { i =>
            coefficients(i) = coefficients(i) - (delta * line.features(i) + learningRate * shrinkage * coefficients(i))
          }
        }
      }
    }
  }

  val baseline = -2
  val nVariables = 5
  val coefficients = Array(1, -1, 1, -1, 1)

  def makeData() = {
    val features = Array.fill(5)(math.random)
    val label = baseline + coefficients.zip(features).map { case (u, v) => u * v }.sum
    new LabeledPoint(label, features)
  }

  def main(args: Array[String]) {
    //get octopus context
    import scala.octopus.OctopusContext._
    val oc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test_ridge"))
      .getOctopusContext

    //create some data on the workers
    val dat = oc.deploy(1 to 100000).map(i => makeData()) //each node has different data here

    //create jobs
    val shrinkages = (-5.0 to 2.0 by 0.5) map { i => math.pow(5, i) }
    val jobs = shrinkages.map(shrinkage => (data: Iterable[LabeledPoint]) => {
      val reg = new RidgeRegression(data, shrinkage)
      (reg.baseline, reg.coefficients)
    })

    //execute jobs and check results
    println("Running " + shrinkages.size + " jobs.")
    val results = dat.execute(jobs)
    results.foreach { case (b, coef) => println(b + "\t" + coef.mkString(",")) }
  }

}
