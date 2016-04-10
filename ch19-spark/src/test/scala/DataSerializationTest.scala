import example.IntWrapper;
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DataSerializationTest extends FunSuite with BeforeAndAfterEach {

  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  var sc: SparkContext = _

  override def beforeEach() {
  }

  override def afterEach() {
    sc.stop()
  }

  test("serializable data class java") {
    sc = new SparkContext("local", "test")

    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)
    assert(result === 6)
  }

  test("serializable data class kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)
    assert(result === 6)
  }

  test("non-serializable data class java") {
    sc = new SparkContext("local", "test")

    intercept[SparkException] {
      sc.parallelize(Array(1, 2, 3))
        .map(new NonSerializableInt(_))
        .reduce((x, y) => x + y)
        .toInt()
    }
  }

  test("non-serializable data class kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(1, 2, 3))
      .map(new NonSerializableInt(_))
      .reduce((x, y) => x + y)
      .toInt()
    assert(result === 6)
  }

  test("avro generic java") {
    sc = new SparkContext("local", "test")

    intercept[SparkException] {
      sc.parallelize(Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3)))
        .map(_.get("value").asInstanceOf[Int])
        .reduce((x, y) => x + y)
    }
  }

  test("avro generic kyro") {
    sc = new SparkContext("local", "test", conf)

    val schema = Schema.create(Schema.Type.INT)
    val result = sc.parallelize(Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3)))
      .map(_.get("value").asInstanceOf[Int])
      .reduce((x, y) => x + y)
    assert(result === 6)
  }

  test("avro specific java") {
    sc = new SparkContext("local", "test")

    intercept[SparkException] {
      sc.parallelize(Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3)))
        .map(_.getValue())
        .reduce((x, y) => x + y)
    }
  }

  test("avro specific kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3)))
      .map(_.getValue())
      .reduce((x, y) => x + y)
    assert(result === 6)
  }

  def toGenericRecord(i: Int): GenericRecord = {
    val avroSchema = new Schema.Parser().parse("{\n  \"type\": \"record\"," +
      "\n  \"name\": \"IntWrapper\"," +
      "\n  \"fields\": [\n    {\"name\": \"value\", \"type\": \"int\"}\n  ]\n}")

    val datum = new GenericData.Record(avroSchema)
    datum.put("value", i)
    datum
  }
}

class NonSerializableInt(val value: Int) {
  def +(that: NonSerializableInt) = new NonSerializableInt(value + that.value)
  def toInt() = value
}