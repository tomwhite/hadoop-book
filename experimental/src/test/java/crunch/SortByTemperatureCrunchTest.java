package crunch;

import java.io.IOException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

public class SortByTemperatureCrunchTest {

  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureAvroCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input/ncdc/sample.txt");
    PTable<Integer, String> temps = records
        .parallelDo(toTempValuePairsFn(), tableOf(ints(), strings()));

    PTable<Integer, String> sorted = Sort.sort(temps, Sort.Order.DESCENDING);
    pipeline.writeTextFile(sorted, "output");
    pipeline.run();
  }

  private static DoFn<String, Pair<Integer, String>> toTempValuePairsFn() {
    return new DoFn<String, Pair<Integer, String>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<Integer, String>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getAirTemperature(), input));
        }
      }
    };
  }
}
