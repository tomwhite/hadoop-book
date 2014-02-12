package crunch;

import java.io.IOException;
import java.io.Serializable;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.junit.Test;

public class SplitCrunchTest implements Serializable {
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input/ncdc/micro-tab/sample_corrupt.txt");

    PCollection<String> goodRecords = records.filter(new FilterFn<String>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override public boolean accept(String s) {
        parser.parse(s);
        return parser.isValidTemperature();
      }
    });

    PCollection<String> badRecords = records.filter(new FilterFn<String>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override public boolean accept(String s) {
        parser.parse(s);
        return !parser.isValidTemperature();
      }
    });

    pipeline.writeTextFile(goodRecords, "output-good");
    pipeline.writeTextFile(badRecords, "output-bad");
    pipeline.run();
  }
}
