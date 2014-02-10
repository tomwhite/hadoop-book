package crunch;

import java.io.IOException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Tuple3;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import org.junit.Test;

import static org.apache.crunch.lib.Sort.Order.ASCENDING;
import static org.apache.crunch.lib.Sort.Order.DESCENDING;
import static org.apache.crunch.lib.Sort.ColumnOrder.by;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.triples;

public class SecondarySortCrunchTest {

  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureAvroCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input/ncdc/sample.txt");
    PCollection<Tuple3<Integer, Integer, String>> triples = records
        .parallelDo(toYearTempValueFn(), triples(ints(), ints(), strings()));

    PCollection<Tuple3<Integer, Integer, String>> sorted =
        Sort.sortTriples(triples, by(1, ASCENDING), by(2, DESCENDING));
    pipeline.writeTextFile(sorted, "output");
    pipeline.run();
  }

  private static DoFn<String, Tuple3<Integer, Integer, String>> toYearTempValueFn() {
    return new DoFn<String, Tuple3<Integer, Integer, String>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Tuple3<Integer, Integer, String>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Tuple3.of(parser.getYearInt(), parser.getAirTemperature(), input));
        }
      }
    };
  }
}
