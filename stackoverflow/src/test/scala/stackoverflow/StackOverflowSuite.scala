package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("StackOverflow").set("spark.driver.bindAddress", "localhost")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List("JavaScript", "PHP")
    override def langSpread = 50000
    override def kmeansKernels = 4
    override def kmeansEta: Double = 2.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `'kmeans' works for given vectors and sampleVectors`: Unit = {
    val vectors: RDD[(Int, Int)] = sc.parallelize(List((0, 10), (0, 20)))
    val sampleVectors: Array[(Int, Int)] = Array((0, 10), (0, 100))
    val means = testObject.kmeans(sampleVectors, vectors)
    assertEquals(means.toList.sorted, List(
      (0,15),
      (0,100)
    ).sorted)
  }

  @Test def `'kmeans' returns proper means`: Unit = {
    val postings = List(
      // ------------
      // JS
      // ------------
      Posting(1, 0, None, None, 100, Option("JavaScript")),
      Posting(2, 1, None, Option(0), 50, Option("JavaScript")),
      // -------------
      Posting(1, 2, None, None, 120, Option("JavaScript")),
      Posting(2, 3, None, Option(2), 60, Option("JavaScript")),
      // -------------
      Posting(1, 4, None, None, 350, Option("JavaScript")),
      Posting(2, 5, None, Option(4), 200, Option("JavaScript")),
      // -------------
      Posting(1, 6, None, None, 120, Option("JavaScript")),
      Posting(2, 7, None, Option(6), 240, Option("JavaScript")),
      // -------------
      Posting(1, 8, None, None, 350, Option("JavaScript")),
      Posting(2, 9, None, Option(8), 260, Option("JavaScript")),
      // -------------
      Posting(1, 10, Option(11), None, 350, Option("JavaScript")),
      Posting(2, 11, None, Option(10), 400, Option("JavaScript")),
      // -------------
      // PHP
      // -------------
      Posting(1, 12, None, None, 120, Option("PHP")),
      Posting(2, 13, None, Option(12), 10, Option("PHP")),
      // -------------
      Posting(1, 14, None, None, 350, Option("PHP")),
      Posting(2, 15, None, Option(14), 20, Option("PHP")),
      // -------------
      Posting(1, 16, None, None, 120, Option("PHP")),
      Posting(2, 17, None, Option(16), 40, Option("PHP")),
      // -------------
      Posting(1, 18, None, None, 350, Option("PHP")),
      Posting(2, 19, None, Option(18), 150, Option("PHP")),
      // -------------
      Posting(1, 20, Option(21), None, 350, Option("PHP")),
      Posting(2, 21, None, Option(20), 180, Option("PHP")),
    )
    val raw     = sc.parallelize(postings)
    val grouped = testObject.groupedPostings(raw)
    val scored  = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored)
    val means   = testObject.kmeans(testObject.sampleVectors(vectors), vectors, debug = true)
//    val results = testObject.clusterResults(means, vectors)

    def sortScored(a: (Question, HighScore), b: (Question, HighScore)): Boolean = a._1.id < b._1.id
    assertEquals(scored.collect.toList.sortWith(sortScored), List(
      (Posting(1,4,None,None,350,Some("JavaScript")),200),
      (Posting(1,16,None,None,120,Some("PHP")),40),
      (Posting(1,14,None,None,350,Some("PHP")),20),
      (Posting(1,0,None,None,100,Some("JavaScript")),50),
      (Posting(1,6,None,None,120,Some("JavaScript")),240),
      (Posting(1,8,None,None,350,Some("JavaScript")),260),
      (Posting(1,12,None,None,120,Some("PHP")),10),
      (Posting(1,18,None,None,350,Some("PHP")),150),
      (Posting(1,20,Some(21),None,350,Some("PHP")),180),
      (Posting(1,10,Some(11),None,350,Some("JavaScript")),400),
      (Posting(1,2,None,None,120,Some("JavaScript")),60)
    ).sortWith(sortScored))

    assertEquals(vectors.collect.toList.sorted, List(
      (0,200),
      (50000,40),
      (50000,20),
      (0,50),
      (0,240),
      (0,260),
      (50000,10),
      (50000,150),
      (50000,180),
      (0,400),
      (0,60)
    ).sorted)

//    assertEquals(testObject.sampleVectors(vectors).toList.sorted, List(
//      (50000,180),
//      (50000,10),
//      (0,60),
//      (0,50)
//    ).sorted)

    assertEquals(means.toList.sorted, List(
      (50000,165),
      (50000,23),
      (0,275),
      (0,55)
    ).sorted)
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
