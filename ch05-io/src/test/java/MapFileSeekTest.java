import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

public class MapFileSeekTest {
  
  private static final String MAP_URI = "test.numbers.map";
  private FileSystem fs;
  private MapFile.Reader reader;
  private WritableComparable<?> key;
  private Writable value;

  @Before
  public void setUp() throws IOException {
    MapFileWriteDemo.main(new String[] { MAP_URI });

    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(MAP_URI), conf);

    reader = new MapFile.Reader(fs, MAP_URI, conf);
    key = (WritableComparable<?>)
      ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(MAP_URI), true);
  }
  
  @Test
  public void get() throws Exception {
    Text value = new Text();
    reader.get(new IntWritable(496), value);
    assertThat(value.toString(), is("One, two, buckle my shoe"));
  }
  
  @Test
  public void seek() throws Exception {
    assertThat(reader.seek(new IntWritable(496)), is(true));
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(497));
    assertThat(((Text) value).toString(), is("Three, four, shut the door"));
  }
  
  

}
