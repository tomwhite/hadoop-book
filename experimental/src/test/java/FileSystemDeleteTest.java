import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class FileSystemDeleteTest {

  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    writeFile(fs, new Path("dir/file"));
  }
  
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    FSDataOutputStream stm = fileSys.create(name);
    stm.close();
  }
  
  @Test
  public void deleteFile() throws Exception {
    assertThat(fs.delete(new Path("dir/file"), false), is(true));
    assertThat(fs.exists(new Path("dir/file")), is(false));
    assertThat(fs.exists(new Path("dir")), is(true));
    assertThat(fs.delete(new Path("dir"), false), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }

  @Test
  public void deleteNonEmptyDirectoryNonRecursivelyFails() throws Exception {
    try {
      fs.delete(new Path("dir"), false);
      fail("Shouldn't delete non-empty directory");
    } catch (IOException e) {
      // expected
    }
  }
  
  @Test
  public void deleteDirectory() throws Exception {
    assertThat(fs.delete(new Path("dir"), true), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }
  
}
