import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SharedDataTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  test("without shared variable") {
    val lookup = Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u")
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup(_))
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  test("broadcast variable") {
    val lookup: Broadcast[Map[Int, String]] =
      sc.broadcast(Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u"))
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup.value(_))
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  test("naive accumulator (doesn't work)") {
    var count = 0
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })
      .reduce((x, y) => x + y)
    assert(count === 0) // not updated to 3
    assert(result === 6)
  }

  test("accumulator") {
    val count: Accumulator[Int] = sc.accumulator(0)
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })
      .reduce((x, y) => x + y)
    assert(count.value === 3)
    assert(result === 6)
  }

}
