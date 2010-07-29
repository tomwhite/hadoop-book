@SuppressWarnings("all")
/** A weather reading. */
public class WeatherRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"WeatherRecord\",\"fields\":[{\"name\":\"year\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":\"int\"},{\"name\":\"stationId\",\"type\":\"string\"}],\"doc\":\"A weather reading.\"}");
  public int year;
  public int temperature;
  public org.apache.avro.util.Utf8 stationId;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return year;
    case 1: return temperature;
    case 2: return stationId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: year = (java.lang.Integer)value$; break;
    case 1: temperature = (java.lang.Integer)value$; break;
    case 2: stationId = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
