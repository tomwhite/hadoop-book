import org.apache.spark.{SparkException, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class FunctionSerializationTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  test("serializable anonymous function") {
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce((x: Int, y: Int) => x + y)
    assert(result === 6)
  }

  test("serializable local function") {
    val myAddFn = (x: Int, y: Int) => x + y
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(myAddFn)
    assert(result === 6)
  }

  test("serializable global singleton function") {
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(Fns.singletonMyAddFn)
    assert(result === 6)
  }

  test("non-serializable class function") {
    val notSerializable = new NotSerializable()
    val params = sc.parallelize(Array(1, 2, 3))
    intercept[SparkException] {
      val result = params.reduce(notSerializable.classMyAddFn)
    }
  }

}

object Fns {
  def singletonMyAddFn(x: Int, y: Int): Int = { x + y }
}
