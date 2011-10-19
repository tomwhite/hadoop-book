import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import junitx.framework.FileAssert;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This test runs the examples and checks that they produce the expected output.
 * It takes each input.txt file and runs it as a script, then tests that the
 * output produced is the same as all the files in output.
 */
@RunWith(Parameterized.class)
public class ExamplesIT {

  private static final File PROJECT_BASE_DIR =
    new File(System.getProperty("hadoop.book.basedir",
        "/Users/tom/workspace/hadoop-book"));
  
  private static final String mode = "local";
  
  private static final String EXAMPLE_DIRS_PROPERTY = "example.dirs";
  private static final String EXAMPLE_DIRS_DEFAULT =
    "ch02/src/main/examples/local," +
    "ch04/src/main/examples/local," +
    "ch05/src/main/examples/local," +
    "ch07/src/main/examples/local," +
    "ch08/src/main/examples/local";

  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> data = new ArrayList<Object[]>();
    String exampleDirs = System.getProperty(EXAMPLE_DIRS_PROPERTY,
        EXAMPLE_DIRS_DEFAULT);
    int i = 0;
    for (String dirName : Splitter.on(',').split(exampleDirs)) {
      File dir = new File(PROJECT_BASE_DIR, dirName);
      if (!dir.exists()) {
        fail(dir + " does not exist");
      }
      for (File file : dir.listFiles()) {
        data.add(new Object[] { file });
        // so we can see which test corresponds to which file
        System.out.printf("%s: %s\n", i++, file);
      }
    }
    return data;
  }
  
  private File example; // parameter
  private File actualOutputDir = new File(PROJECT_BASE_DIR, "output");
  private static Map<String, String> env;
  private static String version;
  
  public ExamplesIT(File example) {
    this.example = example;
  }
  
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws IOException {
    String hadoopHome = System.getenv("HADOOP_HOME");
    assertNotNull("Export the HADOOP_HOME environment variable " +
        "to run the snippet tests", hadoopHome);
    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());
    env.put("HADOOP_HOME", hadoopHome);
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    env.put("HADOOP_CONF_DIR", "snippet/conf/" + mode);
    env.put("HADOOP_CLASSPATH", "hadoop-examples.jar");
    
    String versionOut = execute("hadoop version");
    for (String line : Splitter.on("\n").split(versionOut)) {
      Matcher matcher = Pattern.compile("^Hadoop (.+)+$").matcher(line);
      if (matcher.matches()) {
        version = matcher.group(1);
      }
    }
    assertNotNull("Version not found", version);
  }
  
  @Before
  public void setUp() throws IOException {
    assumeTrue(!example.getPath().endsWith(".ignore"));
    
    if (actualOutputDir.exists()) {
      Files.deleteRecursively(actualOutputDir);
    }
  }
  
  @Test
  public void test() throws Exception {
    File inputFile;
    File expectedOutputDir;

    // First look for data for the particular version
    File versionedExample = new File(example, version);
    if (versionedExample.exists()) {
      inputFile = new File(versionedExample, "input.txt");
      expectedOutputDir = new File(versionedExample, "output");
      // if empty then skip
      assumeTrue(inputFile.exists());
    } else {    // Otherwise use the standard fallback
      inputFile = new File(example, "input.txt");
      expectedOutputDir = new File(example, "output");
    }
    
    String systemOut = execute(inputFile.getAbsolutePath());
    System.out.println(systemOut);
    
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
        File expectedDecompressed = decompress(expectedFile);
        File actualDecompressed = decompress(actualFile);
        FileAssert.assertEquals(expectedDecompressed, actualDecompressed);
      } else {
        FileAssert.assertEquals(expectedFile, actualFile);
      }
    }
  }

  private static String execute(String commandLine) throws ExecuteException, IOException {
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    PumpStreamHandler psh = new PumpStreamHandler(stdout);
    CommandLine cl = CommandLine.parse("/bin/bash " + commandLine);
    DefaultExecutor exec = new DefaultExecutor();
    exec.setWorkingDirectory(PROJECT_BASE_DIR);
    exec.setStreamHandler(psh);
    try {
      exec.execute(cl, env);
    } catch (ExecuteException e) {
      System.out.println(stdout.toString());
      throw e;
    } catch (IOException e) {
      System.out.println(stdout.toString());
      throw e;
    }
    return stdout.toString();
  }

  private File decompress(File file) throws IOException {
    File decompressed = File.createTempFile(getClass().getSimpleName(), ".txt");
    decompressed.deleteOnExit();
    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(file));
    try {
      Files.copy(new InputSupplier<InputStream>() {
          public InputStream getInput() throws IOException {
            return in;
          }
      }, decompressed);
    } finally {
      in.close();
    }
    return decompressed;
  }

}
