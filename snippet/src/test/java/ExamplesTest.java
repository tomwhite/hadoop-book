import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junitx.framework.FileAssert;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ExamplesTest {

  private static File projectBaseDir = new File("/Users/tom/workspace/hadoop-book");
  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> data = new ArrayList<Object[]>();
    File dir = new File(projectBaseDir, "ch08/src/main/examples/local");
    for (File file : dir.listFiles()) {
      data.add(new Object[] { file });
    }
    return data;
  }

  private File example; // parameter
  private File actualOutputDir = new File(projectBaseDir, "output");
  private Map<String, String> env;
  
  public ExamplesTest(File example) {
    this.example = example;
  }
  
  @Before
  public void setUp() throws IOException {
    assumeTrue(!example.getPath().endsWith(".ignore"));

    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());
    env.put("HADOOP_HOME", "/Users/tom/dev/hadoop-0.20.2-cdh3u1");
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    env.put("HADOOP_CONF_DIR", "snippet/bin/local");
    env.put("HADOOP_CLASSPATH", "common/target/classes:ch02/target/classes:ch04/target/classes:ch07/target/classes:ch08/target/classes");
    
    if (actualOutputDir.exists()) {
      Files.deleteRecursively(actualOutputDir);
    }
  }
  
  @Test
  public void test() throws Exception {
    File inputFile = new File(example, "input.txt");
    File expectedOutputDir = new File(example, "output");
    
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    try {
      PumpStreamHandler psh = new PumpStreamHandler(stdout);
      CommandLine cl = CommandLine.parse("/bin/bash " +
          inputFile.getAbsolutePath());
      DefaultExecutor exec = new DefaultExecutor();
      exec.setWorkingDirectory(projectBaseDir);
      exec.setStreamHandler(psh);
      exec.execute(cl, env);
    } finally {
      System.out.println(stdout.toString());
    }
    
    if (!expectedOutputDir.exists()) {
      FileUtils.copyDirectory(actualOutputDir, expectedOutputDir);
      fail(expectedOutputDir  + " does not exist - creating.");
    }
    
    List<File> expectedParts = Lists.newArrayList(
        FileUtils.listFiles(expectedOutputDir, NOT_HIDDEN_FILE_FILTER,
            TrueFileFilter.TRUE));
    List<File> actualParts = Lists.newArrayList(
        FileUtils.listFiles(actualOutputDir, NOT_HIDDEN_FILE_FILTER,
            TrueFileFilter.TRUE));
    assertEquals(expectedParts.size(), actualParts.size());
    
    for (int i = 0; i < expectedParts.size(); i++) {
      File expectedFile = expectedParts.get(i);
      File actualFile = actualParts.get(i);
      if (expectedFile.getPath().endsWith(".gz")) {
        FileAssert.assertBinaryEquals(expectedFile, actualFile);
      } else {
        FileAssert.assertEquals(expectedFile, actualFile);
      }
    }
  }

}
