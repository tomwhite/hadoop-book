import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.HashSet
import scala.reflect.ClassTag

class TransformationsAndActionsTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  test("lazy transformations") {
    val inputPath = "chxx-spark/src/test/resources/fruit.txt"
    val text = sc.textFile(inputPath)
    val lower: RDD[String] = text.map(_.toLowerCase())
    println("Called toLowerCase")
    lower.foreach(println(_))
  }

  test("map reduce") {
    val inputPath = "chxx-spark/src/test/resources/quangle.txt"
    val text: RDD[String] = sc.textFile(inputPath)

    // turn into input key-value pairs
    val input: RDD[(String, Int)] = text.flatMap(_.split(" ")).map(word => (word, 1))

    val mapFn = (kv: (String, Int)) => List(kv)
    val reduceFn = (kv: (String, Iterable[Int])) => List((kv._1, kv._2.sum))
    new MapReduce[String, Int, String, Int, String, Int]()
      .mapReduce(input, mapFn, reduceFn).foreach(println(_))
  }

  test("reduceByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sums: RDD[(String, Int)] = pairs.reduceByKey(_+_)
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  test("foldByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sums: RDD[(String, Int)] = pairs.foldByKey(0)(_+_)
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  test("aggregateByKey") { // Spark 1.1.0
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sets: RDD[(String, HashSet[Int])] =
      pairs.aggregateByKey(new HashSet[Int])(_+=_, _++=_)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

  test("noisy reduceByKey and foldByKey") {
    val pairs: RDD[(String, NoisyInt)] =
      sc.parallelize(Array(("a", new NoisyInt(3)), ("a", new NoisyInt(1)), ("b",
        new NoisyInt(7)), ("a", new NoisyInt(5))))

    val sums: RDD[(String, NoisyInt)] = pairs.reduceByKey(_ + _)
    assert(sums.collect().toSet === Set(("a", new NoisyInt(9)), ("b", new NoisyInt(7))))

    val foldedSums: RDD[(String, NoisyInt)] = pairs.foldByKey(new NoisyInt(0))(_ + _)
    assert(foldedSums.collect().toSet === Set(("a", new NoisyInt(9)), ("b",
      new NoisyInt(7))))
  }

  test("noisy aggregateByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

    val sets: RDD[(String, NoisySet)] =
      pairs.aggregateByKey(new NoisySet)(_+=_, _++=_)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

}

class MapReduce[K1, V1, K2, V2, K3, V3] {
  def mapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
  RDD[(K3, V3)] = {
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey().sortByKey()
    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
    output
  }
}

case class NoisyInt(var value: Int) extends Serializable {
  println("tw: " + hashCode() + " New int: " + value)
  def +(that: NoisyInt) = {
    println("tw: " + hashCode() + " Adding " + value + " to " + that.value + " equals "
      + (value + that.value))
    this.value = value + that.value
    this
  }
  def toInt() = value
}

class NoisySet extends HashSet[Int] {
  println("tw: " + hashCode() + " New empty set")
  override def += (elem: Int): this.type = {
    println("tw: " + hashCode() + " Adding element " + elem + " to " + this)
    super.+=(elem)
    this
  }

  override def ++=(xs: TraversableOnce[Int]): this.type = {
    println("tw: " + hashCode() + " Adding elements + " + xs + " to " + this)
    super.++=(xs)
    this
  }
}