package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features MaxTemperatureWithCountersCrunch and MissingTemperatureFields
// Note that both are naturally combined into a single program.
public class MaxTemperatureWithCountersCrunch {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCountersCrunch <input path> <output path>");
      System.exit(-1);
    }

    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCountersCrunch.class);
    PCollection<String> records = pipeline.readTextFile(args[0]);
    long total = records.getSize();
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, args[1]);
    PipelineResult result = pipeline.run();
    if (result.succeeded()) {
      for (StageResult stageResult : result.getStageResults()) {
        System.out.println(stageResult.getStageName());
        // TODO: point out that we haven't used the MR API for counters (avoids compat issues)
        long missing = stageResult.getCounterValue(Temperature.MISSING);
        long malformed = stageResult.getCounterValue(Temperature.MALFORMED);
        System.out.println("Missing: " + missing);
        System.out.println("Malformed: " + malformed);
        System.out.println("Total: " + total);
        System.out.printf("Records with missing temperature fields: %.2f%%\n",
            100.0 * missing / total);
      }
    }
  }

  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        } else if (parser.isMalformedTemperature()) {
          setStatus("Ignoring possibly corrupt input: " + input);
          increment(Temperature.MALFORMED);
        } else if (parser.isMissingTemperature()) {
          increment(Temperature.MISSING);
        }

        // dynamic counter
        increment("TemperatureQuality", parser.getQuality(), 1);
      }
    };
  }

}
