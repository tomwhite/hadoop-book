import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DatumWriter
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import specific.WeatherRecord

class RDDCreationTest extends FunSuite with BeforeAndAfterEach {

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

  test("parallelized collection") {
    // triangle numbers - but would normally be more compute intensive
    val performExpensiveComputation = (x: Int) => x * (x + 1) / 2

    val params = sc.parallelize(1 to 10)
    val result = params.map(performExpensiveComputation)

    assert(result.collect === (1 to 10).map(performExpensiveComputation))
  }

  test("text file") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)
    val text: RDD[String] = sc.textFile(input.getPath)
    assert(text.collect().toList === List("cherry", "apple", "banana"))
  }

//  test("whole text file") {
//    val inputPath = "ch19-spark/src/test/resources/fruit.txt"
//    val files: RDD[(String, String)] = sc.wholeTextFiles(inputPath)
//    assert(files.first._1.endsWith(inputPath))
//    assert(files.first._2 === "cherry\napple\nbanana\n")
//  }

  test("sequence file writable") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)
    val data = sc.sequenceFile[IntWritable, Text](input.getPath)
    assert(data.first._1 === new IntWritable(100))
    assert(data.first._2 === new Text("One, two, buckle my shoe"))
  }

  test("sequence file java") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)
    val data = sc.sequenceFile[Int, String](input.getPath)
    assert(data.first._1 === 100)
    assert(data.first._2 === "One, two, buckle my shoe")
  }

  test("avro generic data file") {
    val inputPath = "target/data.avro"
    val avroSchema = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"StringPair\",\n  \"doc\": \"A pair of strings.\",\n  \"fields\": [\n    {\"name\": \"left\", \"type\": \"string\"},\n    {\"name\": \"right\", \"type\": \"string\"}\n  ]\n}")

    val datum: GenericRecord = new GenericData.Record(avroSchema)
    datum.put("left", "L")
    datum.put("right", "R")

    val file: File = new File(inputPath)
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(avroSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    AvroJob.setInputKeySchema(job, avroSchema)
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first._1.datum
    assert(record.get("left") === "L")
    assert(record.get("right") === "R")

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2.get("left") === "L")
    assert(record2.get("right") === "R")
  }

  test("avro specific data file") {
    val inputPath = "target/data.avro"

    val datum = new WeatherRecord(2000, 10, "id")

    val file: File = new File(inputPath)
    val writer: DatumWriter[WeatherRecord] = new SpecificDatumWriter[WeatherRecord]()
    val dataFileWriter: DataFileWriter[WeatherRecord] = new DataFileWriter[WeatherRecord](writer)
    dataFileWriter.create(WeatherRecord.getClassSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    AvroJob.setInputKeySchema(job, WeatherRecord.getClassSchema)
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[WeatherRecord]],
      classOf[AvroKey[WeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first._1.datum
    assert(record === datum)

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

  test("avro reflect data file") {
    val inputPath = "target/data.avro"

    val datum: ReflectWeatherRecord = new ReflectWeatherRecord(2000, 10, "id")

    val file: File = new File(inputPath)
    val writer: DatumWriter[ReflectWeatherRecord] = new ReflectDatumWriter[ReflectWeatherRecord]()
    val dataFileWriter: DataFileWriter[ReflectWeatherRecord] = new DataFileWriter[ReflectWeatherRecord](writer)
    dataFileWriter.create(ReflectData.get().getSchema(classOf[ReflectWeatherRecord]), file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    AvroJob.setDataModelClass(job, classOf[ReflectData])
    AvroJob.setInputKeySchema(job, ReflectData.get().getSchema(classOf[ReflectWeatherRecord]))
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[ReflectWeatherRecord]],
      classOf[AvroKey[ReflectWeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first._1.datum
    assert(record === datum)

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

}
