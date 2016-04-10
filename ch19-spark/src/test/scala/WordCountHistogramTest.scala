import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.Map

class WordCountHistogramTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  test("word count histogram") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("set2.txt")), input)
    val hist: Map[Int, Long] = sc.textFile(input.getPath)
      .map(word => (word.toLowerCase(), 1))
      .reduceByKey((a, b) => a + b)
      .map(_.swap)
      .countByKey()
    assert(hist.size === 2)
    assert(hist(1) === 3) // three elements occur once
    assert(hist(2) === 1) // one element occurs twice
  }
}
