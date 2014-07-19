import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RDDPersistenceTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "CustomKryoRegistrator")
    sc = new SparkContext("local", "test", conf)
  }

  override def afterEach() {
    sc.stop()
  }

  test("not enough memory to cache") {
    // triangle numbers - but would normally be more compute intensive
    val performExpensiveComputation = (x: Int) => x * (x + 1) / 2

    val params = sc.parallelize(1 to 10)
    val result = params.map(performExpensiveComputation)

    assert(result.collect === (1 to 10).map(performExpensiveComputation))
  }


}
