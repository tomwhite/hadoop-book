import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.example.data.Group;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ParquetMRWithAvroTest {
  @Rule
  public transient TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();

    File inputFile = tmpDir.newFile("input");
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
        inputFile);
    File parquetOutputFolder = tmpDir.newFolder("output-parquet");
    parquetOutputFolder.delete();
    File textOutputFolder = tmpDir.newFolder("output-text");
    textOutputFolder.delete();

    Path input = new Path(inputFile.toURI());
    Path parquetOutput = new Path(parquetOutputFolder.toURI());
    Path textOutput = new Path(textOutputFolder.toURI());

    int rc = runTool(new TextToParquetWithAvro(), conf, input.toString(), parquetOutput.toString());
    assertEquals(0, rc);

    Path parquetFile = new Path(parquetOutput, "part-m-00000.parquet");
    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFile, readSupport);

    checkNextGroup(reader, 0, "cherry");
    checkNextGroup(reader, 7, "apple");
    checkNextGroup(reader, 13, "banana");
    assertNull(reader.read());

    rc = runTool(new ParquetToTextWithAvro(), conf, parquetOutput.toString(), textOutput.toString());
    assertEquals(0, rc);

    assertEquals(
        "{\"offset\": 0, \"line\": \"cherry\"}\n" +
            "{\"offset\": 7, \"line\": \"apple\"}\n" +
            "{\"offset\": 13, \"line\": \"banana\"}\n",
        Files.toString(new File(new Path(textOutput, "part-m-00000").toUri()),
            Charsets.UTF_8));
  }

  private int runTool(Tool tool, Configuration conf, String... args) throws Exception {
    tool.setConf(conf);
    return tool.run(args);
  }

  private void checkNextGroup(ParquetReader<Group> reader, int offset, String line) throws IOException {
    Group result = reader.read();
    assertNotNull(result);
    assertThat(result.getValueToString(0, 0), is(offset + ""));
    assertThat(result.getString("line", 0), is(line));
  }
}
