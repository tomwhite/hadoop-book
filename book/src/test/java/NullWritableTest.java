import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

public class NullWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    NullWritable writable = NullWritable.get();
    assertThat(serialize(writable).length, is(0));
  }
}
