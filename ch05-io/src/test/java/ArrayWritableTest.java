// == ArrayWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class ArrayWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv ArrayWritableTest
    ArrayWritable writable = new ArrayWritable(Text.class);
    // ^^ ArrayWritableTest
    writable.set(new Text[] { new Text("cat"), new Text("dog") });
    
    TextArrayWritable dest = new TextArrayWritable();
    WritableUtils.cloneInto(dest, writable);
    assertThat(dest.get().length, is(2));
    // TODO: fix cast, also use single assert
    assertThat((Text) dest.get()[0], is(new Text("cat")));
    assertThat((Text) dest.get()[1], is(new Text("dog")));
    
    Text[] copy = (Text[]) dest.toArray();
    assertThat(copy[0], is(new Text("cat")));
    assertThat(copy[1], is(new Text("dog")));
  }
}
