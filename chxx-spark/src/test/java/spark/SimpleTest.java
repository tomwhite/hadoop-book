package spark;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleTest implements Serializable { // needs to be serializable for the Fn
  @Test
  public void test() {
    String file = "src/test/resources/fruit.txt";
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext("local", "Simple App", conf);
    JavaRDD<String> a = sc.textFile(file).cache();

    long numEven = a.filter(new Function<String, Boolean>() {
      public Boolean call(String input) {
        return input.length() % 2 == 0; // even
      }
    }).count();

    assertEquals(2, numEven);

    sc.stop();
  }
}
