import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import specific.WeatherRecord

class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[WeatherRecord])
  }
}
