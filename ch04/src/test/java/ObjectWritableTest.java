import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class ObjectWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    ObjectWritable src = new ObjectWritable(Integer.TYPE, 163);
    ObjectWritable dest = new ObjectWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Integer) dest.get(), is(163));
  }
}
