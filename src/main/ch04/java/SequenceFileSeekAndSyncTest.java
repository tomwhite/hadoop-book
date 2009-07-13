// == SequenceFileSeekAndSyncTest
// == SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
// == SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

public class SequenceFileSeekAndSyncTest {
  
  private static final String SF_URI = "test.numbers.seq";
  private FileSystem fs;
  private SequenceFile.Reader reader;
  private Writable key;
  private Writable value;

  @Before
  public void setUp() throws IOException {
    SequenceFileWriteDemo.main(new String[] { SF_URI });
    
    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(SF_URI), conf);
    Path path = new Path(SF_URI);

    reader = new SequenceFile.Reader(fs, path, conf);
    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(SF_URI), true);
  }

  @Test
  public void seekToRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest
    reader.seek(359);
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(95));
    // ^^ SequenceFileSeekAndSyncTest
  }
  
  @Test(expected=IOException.class)
  public void seekToNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
    reader.seek(360);
    reader.next(key, value); // fails with IOException
    // ^^ SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
  }
  
  @Test
  public void syncFromNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
    reader.sync(360);
    assertThat(reader.getPosition(), is(2021L));
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(59));
    // ^^ SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
  }
  
  @Test
  public void syncAfterLastSyncPoint() throws IOException {
    reader.sync(4557);
    assertThat(reader.getPosition(), is(4788L));
    assertThat(reader.next(key, value), is(false));
  }

}
