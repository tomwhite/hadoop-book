import org.apache.spark.{SparkContext, SparkConf}

class NonSerializableClass {
  def myToLower(s: String): String = { s.toLowerCase() }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Lower")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .map(myToLower(_))
      .saveAsTextFile(args(1))
  }
}
