import org.apache.spark.SparkContext
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

}
