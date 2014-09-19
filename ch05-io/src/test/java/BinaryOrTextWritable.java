import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BinaryOrTextWritable extends GenericWritable {
  private static Class[] TYPES = { BytesWritable.class, Text.class };

  @Override
  @SuppressWarnings("unchecked")
  protected Class<? extends Writable>[] getTypes() {
    return TYPES;
  }
  
}
