import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)
    val text = sc.textFile(input.getPath)
    val lower: RDD[String] = text.map(_.toLowerCase())
    println("Called toLowerCase")
    lower.foreach(println(_))
  }

  test("map reduce") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("quangle.txt")),
      input)
    val text: RDD[String] = sc.textFile(input.getPath)

    // turn into input key-value pairs
    val in: RDD[(String, Int)] = text.flatMap(_.split(" ")).map(word => (word, 1))

    val mapFn = (kv: (String, Int)) => List(kv)
    val reduceFn = (kv: (String, Iterable[Int])) => List((kv._1, kv._2.sum))
    new MapReduce[String, Int, String, Int, String, Int]()
      .naiveMapReduce(in, mapFn, reduceFn).foreach(println(_))
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

  test("aggregateByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sets: RDD[(String, HashSet[Int])] =
      pairs.aggregateByKey(new HashSet[Int])(_+=_, _++=_)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

}

class MapReduce[K1, V1, K2, V2, K3, V3] {
  def naiveMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
  RDD[(K3, V3)] = {
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey().sortByKey()
    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
    output
  }

//  // Spark 1.2.0, see https://issues.apache.org/jira/browse/SPARK-2978
//  def betterMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
//                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
//                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
//  RDD[(K3, V3)] = {
//    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
//    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput
//      .groupByKey()
//      .repartitionAndSortWithinPartitions(Partitioner.defaultPartitioner(mapOutput))
//    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
//    output
//  }
}
