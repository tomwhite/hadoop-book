import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

public class VLongWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    assertThat(serializeToString(new VLongWritable(1)), is("01")); // 1 byte
    assertThat(serializeToString(new VLongWritable(127)), is("7f")); // 1 byte
    assertThat(serializeToString(new VLongWritable(128)), is("8f80")); // 2 byte
    assertThat(serializeToString(new VLongWritable(163)), is("8fa3")); // 2 byte
    assertThat(serializeToString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
    assertThat(serializeToString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
  }
}
