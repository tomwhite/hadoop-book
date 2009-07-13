// == WritableTestBase
// == WritableTestBase-Deserialize
import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableTestBase {
  
  // vv WritableTestBase
  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }
  // ^^ WritableTestBase
  
  // vv WritableTestBase-Deserialize
  public static byte[] deserialize(Writable writable, byte[] bytes)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    writable.readFields(dataIn);
    dataIn.close();
    return bytes;
  }
  // ^^ WritableTestBase-Deserialize
  
  public static String serializeToString(Writable src) throws IOException {
    return StringUtils.byteToHexString(serialize(src));
  }
  
  public static String writeTo(Writable src, Writable dest) throws IOException {
    byte[] data = deserialize(dest, serialize(src));
    return StringUtils.byteToHexString(data);
  }

}
