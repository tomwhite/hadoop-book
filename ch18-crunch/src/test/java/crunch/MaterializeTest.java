package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;

public class MaterializeTest {

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testMaterializeCallsRunImplicitly() throws IOException {
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

    Iterable<String> materialized = lower.materialize();
    for (String s : materialized) { // pipeline is run
      System.out.println(s);
    }
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    PipelineResult result = pipeline.done();
    assertEquals(0, result.getStageResults().size());
  }

  @Test
  public void testPObjectCallsRunImplicitly() throws IOException {
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

    PObject<Collection<String>> po = lower.asCollection();
    for (String s : po.getValue()) { // pipeline is run
      System.out.println(s);
    }
    assertEquals(expectedContent, po.getValue());

    System.out.println("About to call done()");
    PipelineResult result = pipeline.done();
    assertEquals(0, result.getStageResults().size());
  }

}
