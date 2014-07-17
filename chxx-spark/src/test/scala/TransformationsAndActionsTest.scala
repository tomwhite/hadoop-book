import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

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
    val lower = text.map(_.toLowerCase())
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

}
