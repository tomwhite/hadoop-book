import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class GenericWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    BinaryOrTextWritable src = new BinaryOrTextWritable();
    src.set(new Text("text"));
    BinaryOrTextWritable dest = new BinaryOrTextWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(), is(new Text("text")));
    
    src.set(new BytesWritable(new byte[] {3, 5}));
    WritableUtils.cloneInto(dest, src);
    assertThat(((BytesWritable) dest.get()).getLength(), is(2)); // TODO proper assert
  }
}
