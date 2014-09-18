package crunch;

import java.io.Serializable;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

public class SplitCrunch implements Serializable {
  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(SplitCrunch.class);
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
