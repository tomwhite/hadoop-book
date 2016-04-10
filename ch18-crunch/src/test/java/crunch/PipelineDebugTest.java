package crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

public class PipelineDebugTest {
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testDebug() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    pipeline.enableDebug();
    pipeline.getConfiguration().setBoolean("crunch.log.job.progress", true);
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    pipeline.writeTextFile(lines, tmpDir.getFileName("out"));
    pipeline.done();
  }
}
