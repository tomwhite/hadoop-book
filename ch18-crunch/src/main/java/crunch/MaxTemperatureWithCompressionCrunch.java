package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch12-avro MaxTemperatureWithCompression
public class MaxTemperatureWithCompressionCrunch {

  private static final int MISSING = 9999;

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompressionCrunch <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);

    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCompressionCrunch.class, conf);
    PCollection<String> records = pipeline.readTextFile(args[0]);
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());

    pipeline.writeTextFile(maxTemps, args[1]);
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
