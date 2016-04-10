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
        "/Users/tom/book-workspace/hadoop-book"));
  
  private static final String MODE_PROPERTY = "example.mode";
  private static final String MODE_DEFAULT = "local";
  
  private static final String EXAMPLE_CHAPTERS_PROPERTY = "example.chapters";
  private static final String EXAMPLE_CHAPTERS_DEFAULT = "ch02-mr-intro,ch05-io,ch12-avro,ch06-mr-dev,ch08-mr-types,ch09-mr-features";

  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> data = new ArrayList<Object[]>();
    String exampleDirs = System.getProperty(EXAMPLE_CHAPTERS_PROPERTY,
        EXAMPLE_CHAPTERS_DEFAULT);
    int i = 0;
    for (String dirName : Splitter.on(',').split(exampleDirs)) {
      File dir = new File(new File(PROJECT_BASE_DIR, dirName),
          "src/main/examples");
      if (!dir.exists()) {
        fail(dir + " does not exist");
      }
      for (File file : dir.listFiles()) {
        if (file.isDirectory()) {
          data.add(new Object[] { file });
          // so we can see which test corresponds to which file
          System.out.printf("%s: %s\n", i++, file);
        }
      }
    }
    return data;
  }
  
  private File example; // parameter
  private File actualOutputDir = new File(PROJECT_BASE_DIR, "output");
  private static Map<String, String> env;
  private static String version;
  private static String majorVersion;
  private static String mode;
  
  public ExamplesIT(File example) {
    this.example = example;
  }
  
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws IOException {
    mode = System.getProperty(MODE_PROPERTY, MODE_DEFAULT);
    System.out.printf("mode=%s\n", mode);

    String hadoopHome = System.getenv("HADOOP_HOME");
    assertNotNull("Export the HADOOP_HOME environment variable " +
        "to run the snippet tests", hadoopHome);
    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());
    env.put("HADOOP_HOME", hadoopHome);
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    env.put("HADOOP_CONF_DIR", "snippet/conf/" + mode);
    env.put("HADOOP_USER_CLASSPATH_FIRST", "true");
    env.put("HADOOP_CLASSPATH", "hadoop-examples.jar:avro-examples.jar");
    
    System.out.printf("HADOOP_HOME=%s\n", hadoopHome);
    
    String versionOut = execute(hadoopHome + "/bin/hadoop version");
    for (String line : Splitter.on("\n").split(versionOut)) {
      Matcher matcher = Pattern.compile("^Hadoop (.+)+$").matcher(line);
      if (matcher.matches()) {
        version = matcher.group(1);
      }
    }
    assertNotNull("Version not found", version);
    System.out.printf("version=%s\n", version);

    majorVersion = version.substring(0, version.indexOf('.'));
    // Treat 0.2x as Hadoop 1 or 2 for the purposes of these tests
    if ("0".equals(majorVersion)) {
      if (version.startsWith("0.20")) {
        majorVersion = "1";
      } else if (version.startsWith("0.21") || version.startsWith("0.22") ||
          version.startsWith("0.23")) {
        majorVersion = "2";
      }
    }
    assertNotNull("Major version not found", majorVersion);
    System.out.printf("majorVersion=%s\n", majorVersion);

  }
  
  @Before
  public void setUp() throws IOException {
    assumeTrue(!example.getPath().endsWith(".ignore"));
    execute(new File("src/test/resources/setup.sh").getAbsolutePath());
  }
  
  @Test
  public void test() throws Exception {
    System.out.println("Running " + example);
    
    File exampleDir = findBaseExampleDirectory(example);
    File inputFile = new File(exampleDir, "input.txt");
    System.out.println("Running input " + inputFile);
    
    String systemOut = execute(inputFile.getAbsolutePath());
    System.out.println(systemOut);
    
    execute(new File("src/test/resources/copyoutput.sh").getAbsolutePath());
    
    File expectedOutputDir = new File(exampleDir, "output");
    if (!expectedOutputDir.exists()) {
      FileUtils.copyDirectory(actualOutputDir, expectedOutputDir);
      fail(expectedOutputDir  + " does not exist - creating.");
    }
    
    List<File> expectedParts = Lists.newArrayList(
        FileUtils.listFiles(expectedOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    List<File> actualParts = Lists.newArrayList(
        FileUtils.listFiles(actualOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    assertEquals("Number of parts (got " + actualParts + ")",
        expectedParts.size(), actualParts.size());
    
    for (int i = 0; i < expectedParts.size(); i++) {
      File expectedFile = expectedParts.get(i);
      File actualFile = actualParts.get(i);
      if (expectedFile.getPath().endsWith(".gz")) {
        File expectedDecompressed = decompress(expectedFile);
        File actualDecompressed = decompress(actualFile);
        FileAssert.assertEquals(expectedFile.toString(),
            expectedDecompressed, actualDecompressed);
      } else if (expectedFile.getPath().endsWith(".avro")) {
        // Avro files have a random sync marker
        // so just check lengths for the moment
        assertEquals("Avro file length", expectedFile.length(),
            actualFile.length());
      } else {
        FileAssert.assertEquals(expectedFile.toString(),
            expectedFile, actualFile);
      }
    }
    System.out.println("Completed " + example);
  }
  
  private File findBaseExampleDirectory(File example) {
    // Look in base/<version>/<mode> then base/<version> then base/<major-version>/<mode>
    // then base/<major-version> then base/<mode>
    File[] candidates = {
        new File(new File(example, version), mode),
        new File(example, version),
        new File(new File(example, majorVersion), mode),
        new File(example, majorVersion),
        new File(example, mode),
    };
    for (File candidate : candidates) {
      if (candidate.exists()) {
        File inputFile = new File(candidate, "input.txt");
        // if no input file then skip test
        assumeTrue(inputFile.exists());
        return candidate;
      }
    }
    return example;
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
