// == IntWritableTest
// == IntWritableTest-ValueConstructor
// == IntWritableTest-SerializedLength
// == IntWritableTest-SerializedBytes
// == IntWritableTest-Deserialization
// == IntWritableTest-Comparator
// == IntWritableTest-ObjectComparison
// == IntWritableTest-BytesComparison
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class IntWritableTest extends WritableTestBase {
  
  @Test
  public void walkthroughWithNoArgsConstructor() throws IOException {
    // vv IntWritableTest
    IntWritable writable = new IntWritable();
    writable.set(163);
    // ^^ IntWritableTest
    checkWalkthrough(writable);
  }

  @Test
  public void walkthroughWithValueConstructor() throws IOException {
    // vv IntWritableTest-ValueConstructor
    IntWritable writable = new IntWritable(163);
    // ^^ IntWritableTest-ValueConstructor
    checkWalkthrough(writable);
  }

  private void checkWalkthrough(IntWritable writable) throws IOException {
    // vv IntWritableTest-SerializedLength
    byte[] bytes = serialize(writable);
    assertThat(bytes.length, is(4));
    // ^^ IntWritableTest-SerializedLength
    
    // vv IntWritableTest-SerializedBytes
    assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
    // ^^ IntWritableTest-SerializedBytes
    
    // vv IntWritableTest-Deserialization
    IntWritable newWritable = new IntWritable();
    deserialize(newWritable, bytes);
    assertThat(newWritable.get(), is(163));
    // ^^ IntWritableTest-Deserialization
  }
  
  @Test
  public void comparator() throws IOException {
    // vv IntWritableTest-Comparator
    RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
    // ^^ IntWritableTest-Comparator
    
    // vv IntWritableTest-ObjectComparison
    IntWritable w1 = new IntWritable(163);
    IntWritable w2 = new IntWritable(67);
    assertThat(comparator.compare(w1, w2), greaterThan(0));
    // ^^ IntWritableTest-ObjectComparison
    
    // vv IntWritableTest-BytesComparison
    byte[] b1 = serialize(w1);
    byte[] b2 = serialize(w2);
    assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length),
        greaterThan(0));
    // ^^ IntWritableTest-BytesComparison
  }
  
  @Test
  public void test() throws IOException {
    IntWritable src = new IntWritable(163);
    IntWritable dest = new IntWritable();
    assertThat(writeTo(src, dest), is("000000a3"));
    assertThat(dest.get(), is(src.get()));
  }

}
