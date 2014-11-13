package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features SortByTemperatureUsingTotalOrderPartitioner
public class SortByTemperatureCrunch {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureCrunch <input path> <output path>");
      System.exit(-1);
    }
    Pipeline pipeline = new MRPipeline(SortByTemperatureCrunch.class);
    PCollection<String> records = pipeline.readTextFile(args[0]);
    PTable<Integer, String> temps = records
        .parallelDo(toTempValuePairsFn(), tableOf(ints(), strings()));

    PTable<Integer, String> sorted = Sort.sort(temps, Sort.Order.DESCENDING);
    pipeline.writeTextFile(sorted, args[1]);
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
