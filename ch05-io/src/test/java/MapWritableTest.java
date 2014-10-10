// == MapWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class MapWritableTest extends WritableTestBase {
  
  @Test
  public void mapWritable() throws IOException {
    // vv MapWritableTest
    MapWritable src = new MapWritable();
    src.put(new IntWritable(1), new Text("cat"));
    src.put(new VIntWritable(2), new LongWritable(163));
    
    MapWritable dest = new MapWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(new IntWritable(1)), is(new Text("cat")));
    assertThat((LongWritable) dest.get(new VIntWritable(2)),
        is(new LongWritable(163)));
    // ^^ MapWritableTest
  }

  @Test
  public void setWritableEmulation() throws IOException {
    MapWritable src = new MapWritable();
    src.put(new IntWritable(1), NullWritable.get());
    src.put(new IntWritable(2), NullWritable.get());
    
    MapWritable dest = new MapWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat(dest.containsKey(new IntWritable(1)), is(true));
  }
}
