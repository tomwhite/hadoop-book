import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.*;

/**
 * Create a file with 4k blocksize, 3 blocks, lines 1023 bytes + 1 byte nl
 *   Make each line begin with its line number 01 to 12
 * Then expect to get 3 splits (one per block), and 4 records per split
 *   * each split corresponds exactly to one block
 * If lines are 1024 * 1.5 bytes long (in nl), then what do we get for each record?
 *   * Do we lose records?
 *   * If not then in general need to get end from another block?
 *   
 * How does compression fit in!?
 */
/*

Line
0   0
1   1024
2   2048
3   

 */
public class SplitTest {
  
  private static final Random r = new Random();
  
  private static final String[] lines1 = new String[120];
  static {
    for (int i = 0; i < lines1.length; i++) {
      char[] c = new char[1023];
      c[0] = Integer.toHexString(i % 16).charAt(0);
      for (int j = 1; j < c.length; j++) {
        c[j] = (char) (r.nextInt(26) + (int) 'a');
      }
      lines1[i] = new String(c);
    }
  }
  
  private static final String[] lines2 = new String[12];
  static {
    for (int i = 0; i < lines2.length; i++) {
      char[] c = new char[1023 + 512];
      c[0] = Integer.toHexString(i % 16).charAt(0);
      for (int j = 1; j < c.length; j++) {
        c[j] = (char) (r.nextInt(26) + (int) 'a');
      }
      lines2[i] = new String(c);
    }
  }
  
  private static MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException {
    Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  @Test
  @Ignore("Needs more investigation")
  public void recordsCoincideWithBlocks() throws IOException {
    int recordLength = 1024;
    Path input = new Path("input");
    createFile(input, 12, recordLength);
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    assertThat(splits.length, is(3));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    checkSplit(splits[2], 8192, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 4);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 4, 8);
    checkRecordReader(inputFormat, splits[2], job, recordLength, 8, 12);
  }
  
  @Test
  public void recordsDontCoincideWithBlocks() throws IOException {
    int recordLength = 1024 + 512;
    Path input = new Path("input");
    createFile(input, 8, recordLength);
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    System.out.println(Arrays.asList(splits));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    checkSplit(splits[2], 8192, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 3);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 3, 6);
    checkRecordReader(inputFormat, splits[2], job, recordLength, 6, 8);

  }
  
  @Test
  @Ignore("Needs more investigation")
  public void compression() throws IOException {
    int recordLength = 1024;
    Path input = new Path("input.bz2");
    createFile(input, 24, recordLength);
    System.out.println(">>>>>>" + fs.getLength(input));
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    System.out.println(Arrays.asList(splits));
    assertThat(splits.length, is(2));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 4);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 5, 12);

  }


  private void checkSplit(InputSplit split, long start, long length) {
    assertThat(split, instanceOf(FileSplit.class));
    FileSplit fileSplit = (FileSplit) split;
    assertThat(fileSplit.getStart(), is(start));
    assertThat(fileSplit.getLength(), is(length));
  }
  
  private void checkRecord(int record, RecordReader<LongWritable, Text> recordReader, long expectedKey, String expectedValue)
      throws IOException {
    LongWritable key = new LongWritable();
    Text value = new Text();
    assertThat(recordReader.next(key, value), is(true));
    assertThat("Record " + record, value.toString(), is(expectedValue));
    assertThat("Record " + record, key.get(), is(expectedKey));
  }
  
  private void checkRecordReader(InputFormat<LongWritable, Text> inputFormat,
      InputSplit split, JobConf job, long recordLength, int startLine, int endLine) throws IOException {
    RecordReader<LongWritable, Text> recordReader =
      inputFormat.getRecordReader(split, job, Reporter.NULL);
    for (int i = startLine; i < endLine; i++) {
      checkRecord(i, recordReader, i * recordLength, line(i, recordLength));
    }
    assertThat(recordReader.next(new LongWritable(), new Text()), is(false));
  }

  private void createFile(Path input, int records, int recordLength) throws IOException {
    long fileSize = 4096;
    OutputStream out = fs.create(input, true, 4096, (short) 1, fileSize);
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
    CompressionCodec codec = codecFactory.getCodec(input);
    if (codec != null) {
      out = codec.createOutputStream(out);
    }
    Writer writer = new OutputStreamWriter(out);
    try {
      for (int n = 0; n < records; n++) {
        writer.write(line(n, recordLength));
        writer.write("\n");
      }
    } finally {
      writer.close();
    }
  }

  private String line(int i, long recordLength) {
    return recordLength == 1024 ? lines1[i] : lines2[i];
  }

}
