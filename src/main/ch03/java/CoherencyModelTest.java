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
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
  }
  
  @After
  public void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  @Test
  public void fileExistsImmediatelyAfterCreation() throws IOException {
    // vv CoherencyModelTest
    Path p = new Path("p");
    fs.create(p);
    assertThat(fs.exists(p), is(true));
    // ^^ CoherencyModelTest
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
  @Ignore("See https://issues.apache.org/jira/browse/HADOOP-4379")
  public void fileContentIsVisibleAfterFlushAndSync() throws IOException {
    // vv CoherencyModelTest-VisibleAfterFlushAndSync
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    out.flush();
    /*[*/out.sync();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterFlushAndSync
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }
  
  
  @Test
  public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
    File localFile = File.createTempFile("tmp", "");
    assertThat(localFile.exists(), is(true));
    // vv CoherencyModelTest-LocalFileVisibleAfterFlush
    FileOutputStream out = new FileOutputStream(localFile);
    out.write("content".getBytes("UTF-8"));
    out.flush(); // flush to operating system
    out.getFD().sync(); // sync to disk
    assertThat(localFile.length(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-LocalFileVisibleAfterFlush
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
