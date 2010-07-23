// == FileSystemGlobTest
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.*;

public class FileSystemGlobTest {
  
  private static final String BASE_PATH = "/tmp/" +
    FileSystemGlobTest.class.getSimpleName();
  
  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    fs.mkdirs(new Path(BASE_PATH, "2007/12/30"));
    fs.mkdirs(new Path(BASE_PATH, "2007/12/31"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/01"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/02"));
  }
  
  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(BASE_PATH), true);
  }
  
  @Test
  public void glob() throws Exception {
    assertThat(glob("/*"), is(paths("/2007", "/2008")));
    assertThat(glob("/*/*"), is(paths("/2007/12", "/2008/01")));
    
    // bug?
    //assertThat(glob("/*/12/*"), is(paths("/2007/12/30", "/2007/12/31")));

    assertThat(glob("/200?"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[78]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[7-8]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[^01234569]"), is(paths("/2007", "/2008")));

    assertThat(glob("/*/*/{31,01}"), is(paths("/2007/12/31", "/2008/01/01")));
    assertThat(glob("/*/*/3{0,1}"), is(paths("/2007/12/30", "/2007/12/31")));

    assertThat(glob("/*/{12/31,01/01}"), is(paths("/2007/12/31", "/2008/01/01")));

    // bug?
    //assertThat(glob("/2007/12/30/data\\[2007-12-30\\]"), is(paths("/2007/12/30/data[2007-12-30]")));

  }
  
  @Test
  public void regexIncludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$")), is(paths("/2007")));
    
    // bug?
    //assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007/12/31$")), is(paths("/2007/12/31")));
    // this works but shouldn't be necessary? see https://issues.apache.org/jira/browse/HADOOP-3497
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007(/12(/31)?)?$")), is(paths("/2007/12/31")));
  }
  
  @Test
  public void regexExcludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$", false)), is(paths("/2008")));
    assertThat(glob("/2007/*/*", new RegexPathFilter("^.*/2007/12/31$", false)), is(paths("/2007/12/30")));
  }
  
  @Test
  public void regexExcludesWithRegexExcludePathFilter() throws Exception {
    assertThat(glob("/*", new RegexExcludePathFilter("^.*/2007$")), is(paths("/2008")));
    assertThat(glob("/2007/*/*", new RegexExcludePathFilter("^.*/2007/12/31$")), is(paths("/2007/12/30")));
  }
	  
  public void testDateRange() throws Exception {
    DateRangePathFilter filter = new DateRangePathFilter(date("2007/12/31"),
        date("2008/01/01"));
    assertThat(glob("/*/*/*", filter), is(paths("/2007/12/31", "/2008/01/01")));  
  } 
  
  private Path[] glob(String pattern) throws IOException {
    return FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern)));
  }
  
  private Path[] glob(String pattern, PathFilter pathFilter) throws IOException {
    return FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern), pathFilter));
  }
  
  private Path[] paths(String... pathStrings) {
    Path[] paths = new Path[pathStrings.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("file:" + BASE_PATH + pathStrings[i]);
    }
    return paths;
  }
  
  private Date date(String date) throws ParseException {
    return new SimpleDateFormat("yyyy/MM/dd").parse(date);
  }
}
