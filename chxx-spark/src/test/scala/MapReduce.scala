import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MapReduce[K1, V1, K2, V2, K3, V3] {
  def mapReduce[K2: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
        mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
        reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
        RDD[(K3, V3)] = {
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
    val grouped: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey()
    val output: RDD[(K3, V3)] = grouped.flatMap(reduceFn)
    output
  }
}

