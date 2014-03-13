package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;

public class SerializableFunctionsTest { // not serializable

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  @Test(expected = CrunchRuntimeException.class)
  public void testInnerClassIsNotSerializable() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(SerializableFunctionsTest.class);
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    lines.parallelDo(new DoFn<String, String>() {
          @Override
          public void process(String input, Emitter<String> emitter) {
            emitter.emit(input);
          }
        }, strings())
      .materialize().iterator();
    pipeline.done();
  }

  @Test
  public void testInitialize() throws IOException {
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(SerializableFunctionsTest.class);
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    Iterable<String> materialized = lines.filter(new PatternFilterFn()).materialize();
    assertEquals(expectedContent, Lists.newArrayList(materialized));
    pipeline.done();
  }
}
