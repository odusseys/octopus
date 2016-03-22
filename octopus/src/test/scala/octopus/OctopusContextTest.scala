package scala.octopus

import org.scalatest.FunSuite

/**
 * Created by umizrahi on 02/03/2016.
 */
class OctopusContextTest extends FunSuite {

  import OctopusContextTest._

  test("Should be able to run multiple jobs concurrently, and similar jobs should run in similar time") {
    val jobs = Seq(job _, job _)
    val results1 = testContext.submitJobs(jobs)
    val results2 = testContext.submitJobs(jobs)
    var done1 = false
    var done2 = false
    var done1Time = 0L
    var done2Time = 0L
    val startTime = System.currentTimeMillis()
    while (!(done1 && done2)) {
      if (!done1 && results1.isDone) {
        done1Time = System.currentTimeMillis()
        done1 = true
      }
      if (!done2 && results2.isDone) {
        done2Time = System.currentTimeMillis()
        done2 = true
      }
      Thread.sleep(100)
    }
    val time1 = done1Time - startTime
    val time2 = done2Time - startTime
    assert(math.abs(time1 - time2) < 0.25 * math.max(time1, time2))
    //note : in this test, time1 and time2 have a 100ms resolution due to the Thread.sleep,
    // so the computing time should be much greater
  }

  test("Should be able to run multiple jobs concurrently, and shorter jobs should finish earlier") {
    val jobs = Seq(job _, job _)
    val results1 = testContext.submitJobs(jobs)
    val results2 = testContext.submitJobs(Seq[() => Int]())
    var done1 = false
    var done2 = false
    var done1Time = 0L
    var done2Time = 0L
    val startTime = System.currentTimeMillis()
    while (!(done1 && done2)) {
      if (!done1 && results1.isDone) {
        done1Time = System.currentTimeMillis()
        done1 = true
      }
      if (!done2 && results2.isDone) {
        done2Time = System.currentTimeMillis()
        done2 = true
      }
      Thread.sleep(100)
    }
    val time1 = done1Time - startTime
    val time2 = done2Time - startTime
    assert(2 * time2 < time1)
    //note : in this test, time1 and time2 have a 100ms resolution due to the Thread.sleep,
    // so the computing time should be much greater
  }

}


object OctopusContextTest {
  def job = {
    var t = 0.0
    (1 to 1000000).foreach(i => {
      t = t + math.random * i / 100000
    })
    t
  }
}
