import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.junit.*;


public class TextInputFormatsTest {
  
  private static final String BASE_PATH = "/tmp/" +
    TextInputFormatsTest.class.getSimpleName();
  
  private JobConf conf;
  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    conf = new JobConf();
    fs = FileSystem.get(conf);
  }
  
  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(BASE_PATH), true);
  }

  @Test
  public void text() throws Exception {
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    writeInput(input);
    
    TextInputFormat format = new TextInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    checkNextLine(recordReader, 57, "But his face you could not see,");
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }

  @Test
  public void keyValue() throws Exception {
    String input =
      "line1\tOn the top of the Crumpetty Tree\n" +
      "line2\tThe Quangle Wangle sat,\n" +
      "line3\tBut his face you could not see,\n" +
      "line4\tOn account of his Beaver Hat.";
    
    writeInput(input);
    
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader<Text, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, "line1", "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, "line2", "The Quangle Wangle sat,");
    checkNextLine(recordReader, "line3", "But his face you could not see,");
    checkNextLine(recordReader, "line4", "On account of his Beaver Hat.");
  }
  
  @Test
  public void nLine() throws Exception {
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    writeInput(input);
    
    conf.setInt("mapred.line.input.format.linespermap", 2);
    NLineInputFormat format = new NLineInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 2);
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    recordReader = format.getRecordReader(splits[1], conf, Reporter.NULL);
    checkNextLine(recordReader, 57, "But his face you could not see,");
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }
  
  private void writeInput(String input) throws IOException {
    OutputStream out = fs.create(new Path(BASE_PATH, "input"));
    out.write(input.getBytes());
    out.close();
    
    FileInputFormat.setInputPaths(conf, BASE_PATH);
  }
  
  private void checkNextLine(RecordReader<LongWritable, Text> recordReader,
      long expectedKey, String expectedValue) throws IOException {
    LongWritable key = new LongWritable();
    Text value = new Text();
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    assertThat(key.get(), is(expectedKey));
    assertThat(value.toString(), is(expectedValue));
  }
  
  private void checkNextLine(RecordReader<Text, Text> recordReader,
      String expectedKey, String expectedValue) throws IOException {
    Text key = new Text();
    Text value = new Text();
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    assertThat(key.toString(), is(expectedKey));
    assertThat(value.toString(), is(expectedValue));
  }
}
