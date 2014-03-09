package crunch;

import java.io.IOException;
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

public class SerializableFunctionsTest { // not serializable

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  @Test(expected = CrunchRuntimeException.class)
  public void testInnerClassIsNotSerializable() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(RunSemanticsTest.class);
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
}
