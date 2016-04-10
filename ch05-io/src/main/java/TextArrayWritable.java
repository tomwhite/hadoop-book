// == TextArrayWritable
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

// vv TextArrayWritable
public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(Text.class);
  }
}
// ^^ TextArrayWritable
