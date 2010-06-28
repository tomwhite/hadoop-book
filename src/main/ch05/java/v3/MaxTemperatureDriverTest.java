package v3;
// cc MaxTemperatureDriverTestV3 A test for MaxTemperatureDriver that uses a local, in-process job runner
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.junit.Test;

public class MaxTemperatureDriverTest {
  
  public static class OutputLogFilter implements PathFilter {
    public boolean accept(Path path) {
      return !path.getName().startsWith("_");
    }
  }
  
//vv MaxTemperatureDriverTestV3
  @Test
  public void test() throws Exception {
    JobConf conf = new JobConf();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    
    Path input = new Path("input/ncdc/micro");
    Path output = new Path("output");
    
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(output, true); // delete old output
    
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);
    
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    assertThat(exitCode, is(0));
    
    checkOutput(conf, output);
  }
//^^ MaxTemperatureDriverTestV3

  private void checkOutput(JobConf conf, Path output) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(output, new OutputLogFilter()));
    assertThat(outputFiles.length, is(1));
    
    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
    BufferedReader expected = asBufferedReader(
        getClass().getResourceAsStream("expected.txt"));
    String expectedLine;
    while ((expectedLine = expected.readLine()) != null) {
      assertThat(actual.readLine(), is(expectedLine));
    }
    assertThat(actual.readLine(), nullValue());
    actual.close();
    expected.close();
  }
  
  private BufferedReader asBufferedReader(InputStream in) throws IOException {
    return new BufferedReader(new InputStreamReader(in));
  }
}
