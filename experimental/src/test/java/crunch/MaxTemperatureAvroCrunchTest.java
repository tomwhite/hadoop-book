package crunch;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;

import static org.apache.crunch.types.avro.Avros.*;

// TODO: consider what is transient - need to write about it
public class MaxTemperatureAvroCrunchTest implements Serializable {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{" +
          "  \"type\": \"record\"," +
          "  \"name\": \"WeatherRecord\"," +
          "  \"doc\": \"A weather reading.\"," +
          "  \"fields\": [" +
          "    {\"name\": \"year\", \"type\": \"int\"}," +
          "    {\"name\": \"temperature\", \"type\": \"int\"}," +
          "    {\"name\": \"stationId\", \"type\": \"string\"}" +
          "  ]" +
          "}"
  );
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureAvroCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input/ncdc/sample.txt");
    
    PTable<Integer, GenericData.Record> maxTemps = records
      .parallelDo(toYearRecordPairsFn(), tableOf(ints(), generics(SCHEMA)))
      .groupByKey()
      .combineValues(new Aggregators.SimpleAggregator<GenericData.Record>() {
        transient GenericData.Record max;
        @Override
        public void reset() {
          max = null;
        }

        @Override
        public void update(GenericData.Record value) {
          if (max == null ||
              (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
            max = newWeatherRecord(value);
          }
        }

        private GenericData.Record newWeatherRecord(GenericData.Record value) {
          GenericData.Record record = new GenericData.Record(SCHEMA);
          record.put("year", value.get("year"));
          record.put("temperature", value.get("temperature"));
          record.put("stationId", value.get("stationId"));
          return record;
        }

        @Override
        public Iterable<GenericData.Record> results() {
          return Collections.singleton(max);
        }
      });
    
    pipeline.writeTextFile(maxTemps, "output");
    pipeline.run();
  }

  private static DoFn<String, Pair<Integer, GenericData.Record>> toYearRecordPairsFn() {
    return new DoFn<String, Pair<Integer, GenericData.Record>>() {
      private NcdcRecordParser parser = new NcdcRecordParser();
      private transient GenericData.Record record;
      @Override
      public void process(String input, Emitter<Pair<Integer, GenericData.Record>> emitter) {
        parser.parse(input.toString());
        if (parser.isValidTemperature()) {
          if (record == null) {
            record = new GenericData.Record(SCHEMA);
          }
          record.put("year", parser.getYearInt());
          record.put("temperature", parser.getAirTemperature());
          record.put("stationId", parser.getStationId());
          emitter.emit(Pair.of(parser.getYearInt(), record));
        }
      }
    };
  }

}
