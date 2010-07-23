import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

public class BooleanWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    BooleanWritable src = new BooleanWritable(true);
    BooleanWritable dest = new BooleanWritable();
    assertThat(writeTo(src, dest), is("01"));
    assertThat(dest.get(), is(src.get()));
  }
}
