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
import org.junit.Test;

import java.io.IOException;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

public class MaxTemperatureWithCountersCrunchTest {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCountersCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input/ncdc/all");
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, "output");
    PipelineResult result = pipeline.run();
    if (result.succeeded()) {
      for (StageResult stageResult : result.getStageResults()) {
        System.out.println(stageResult.getStageName());
        // TODO: point out that we haven't used the MR API, which is good
        System.out.println("Missing: " +
            stageResult.getCounterValue(Temperature.MISSING));
        System.out.println("Malformed: " +
            stageResult.getCounterValue(Temperature.MALFORMED));
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
      }
    };
  }

}
