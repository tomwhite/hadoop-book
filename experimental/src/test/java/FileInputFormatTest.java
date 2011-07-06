import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;


public class FileInputFormatTest {
  private static final String BASE_PATH = "/Users/tom/workspace/htdg/input/fileinput";
  
  @Test(expected = IOException.class)
  @Ignore("See HADOOP-5588")
  public void directoryWithSubdirectory() throws Exception {
    JobConf conf = new JobConf();
    
    Path path = new Path(BASE_PATH, "dir");
    FileInputFormat.addInputPath(conf, path);

    conf.getInputFormat().getSplits(conf, 1);
  }
  
  @Test
  @Ignore("See HADOOP-5588")
  public void directoryWithSubdirectoryUsingGlob() throws Exception {
    JobConf conf = new JobConf();
    
    Path path = new Path(BASE_PATH, "dir/a*");
    FileInputFormat.addInputPath(conf, path);

    InputSplit[] splits = conf.getInputFormat().getSplits(conf, 1);
    assertThat(splits.length, is(1));
  }
  
  @Test
  public void inputPathProperty() throws Exception {
    JobConf conf = new JobConf();
    FileInputFormat.setInputPaths(conf, new Path("/{a,b}"), new Path("/{c,d}"));
    assertThat(conf.get("mapred.input.dir"), is("file:/{a\\,b},file:/{c\\,d}"));
  }
  
}
