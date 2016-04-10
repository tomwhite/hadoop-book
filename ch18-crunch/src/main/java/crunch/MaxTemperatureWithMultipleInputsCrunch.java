package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch08-mr-types MaxTemperatureWithMultipleInputsCrunch
// TODO: sanity check output
public class MaxTemperatureWithMultipleInputsCrunch {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MaxTemperatureWithMultipleInputsCrunch <ncdc input> <metoffice input> <output>");
      System.exit(-1);
    }

    Pipeline pipeline = new MRPipeline(MaxTemperatureWithMultipleInputsCrunch.class);

    PTable<String, Integer> ncdc = pipeline.readTextFile(args[0])
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    PTable<String, Integer> metOffice = pipeline.readTextFile(args[1])
        .parallelDo(metOfficeToYearTempPairsFn(), tableOf(strings(), ints()));

    PTable<String, Integer> maxTemps = ncdc
      .union(metOffice)
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, args[2]);
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
