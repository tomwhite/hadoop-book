// == CoherencyModelTest
// == CoherencyModelTest-NotVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterFlushAndSync
// == CoherencyModelTest-LocalFileVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterClose
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

public class CoherencyModelTest {
  private MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
  }
  
  @After
  public void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  @Test
  public void fileExistsImmediatelyAfterCreation() throws IOException {
    // vv CoherencyModelTest-ExistsImmediatelyAfterCreation
    Path p = new Path("p");
    fs.create(p);
    assertThat(fs.exists(p), is(true));
    // ^^ CoherencyModelTest-ExistsImmediatelyAfterCreation
    assertThat(fs.delete(p, true), is(true));
  }
  
  @Test
  public void fileContentIsNotVisibleAfterFlush() throws IOException {
    // vv CoherencyModelTest-NotVisibleAfterFlush
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.flush();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(0L));
    // ^^ CoherencyModelTest-NotVisibleAfterFlush
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }
  
  @Test
  public void fileContentIsVisibleAfterHFlush() throws IOException {
    // vv CoherencyModelTest-VisibleAfterHFlush
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hflush();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterHFlush
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }

  @Test
  public void fileContentIsVisibleAfterHSync() throws IOException {
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hsync();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }

  @Test
  public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
    File localFile = File.createTempFile("tmp", "");
    assertThat(localFile.exists(), is(true));
    // vv CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    FileOutputStream out = new FileOutputStream(localFile);
    out.write("content".getBytes("UTF-8"));
    out.flush(); // flush to operating system
    out.getFD().sync(); // sync to disk
    assertThat(localFile.length(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    out.close();
    assertThat(localFile.delete(), is(true));
  }
  
  @Test
  public void fileContentIsVisibleAfterClose() throws IOException {
    // vv CoherencyModelTest-VisibleAfterClose
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.close();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterClose
    assertThat(fs.delete(p, true), is(true));
  }

}
