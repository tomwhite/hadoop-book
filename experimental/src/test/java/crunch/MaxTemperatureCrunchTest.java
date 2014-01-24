package crunch;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

import java.io.IOException;

import org.apache.crunch.fn.Aggregators;
import org.junit.Test;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

public class MaxTemperatureCrunchTest {
  
  private static final int MISSING = 9999;
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input");
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, "output");
    pipeline.run();
  }

  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        String line = input.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
          airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
          airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
          emitter.emit(Pair.of(year, airTemperature));
        }
      }
    };
  }

}
