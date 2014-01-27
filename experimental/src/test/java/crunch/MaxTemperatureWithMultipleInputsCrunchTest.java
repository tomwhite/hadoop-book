package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.junit.Test;

import java.io.IOException;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// TODO: sanity check output
public class MaxTemperatureWithMultipleInputsCrunchTest {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureWithMultipleInputsCrunchTest.class);

    PTable<String, Integer> ncdc = pipeline.readTextFile("input/ncdc/all")
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    PTable<String, Integer> metOffice = pipeline.readTextFile("input/metoffice")
        .parallelDo(metOfficeToYearTempPairsFn(), tableOf(strings(), ints()));

    PTable<String, Integer> maxTemps = ncdc
      .union(metOffice)
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, "output");
    pipeline.run();
  }

  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }

  private static DoFn<String, Pair<String, Integer>> metOfficeToYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      MetOfficeRecordParser parser = new MetOfficeRecordParser();
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }
}
