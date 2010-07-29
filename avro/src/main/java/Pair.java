@SuppressWarnings("all")
/** A pair of strings. */
public class Pair extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Pair\",\"fields\":[{\"name\":\"left\",\"type\":\"string\"},{\"name\":\"right\",\"type\":\"string\"}],\"doc\":\"A pair of strings.\"}");
  public org.apache.avro.util.Utf8 left;
  public org.apache.avro.util.Utf8 right;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return left;
    case 1: return right;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: left = (org.apache.avro.util.Utf8)value$; break;
    case 1: right = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
