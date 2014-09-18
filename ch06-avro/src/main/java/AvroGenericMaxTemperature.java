// cc AvroGenericMaxTemperature MapReduce program to find the maximum temperature, creating Avro output

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv AvroGenericMaxTemperature
public class AvroGenericMaxTemperature extends Configured implements Tool {
  
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

  public static class MaxTemperatureMapper
      extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private GenericRecord record = new GenericData.Record(SCHEMA);
    @Override
    public void map(Utf8 line,
        AvroCollector<Pair<Integer, GenericRecord>> collector,
        Reporter reporter) throws IOException {
      parser.parse(line.toString());
      if (parser.isValidTemperature()) {
        record.put("year", parser.getYearInt());
        record.put("temperature", parser.getAirTemperature());
        record.put("stationId", parser.getStationId());
        collector.collect(
            new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
      }
    }
  }
  
  public static class MaxTemperatureReducer
      extends AvroReducer<Integer, GenericRecord, GenericRecord> {

    @Override
    public void reduce(Integer key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter)
        throws IOException {
      GenericRecord max = null;
      for (GenericRecord value : values) {
        if (max == null || 
            (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
          max = newWeatherRecord(value);
        }
      }
      collector.collect(max);
    }
    private GenericRecord newWeatherRecord(GenericRecord value) {
      GenericRecord record = new GenericData.Record(SCHEMA);
      record.put("year", value.get("year"));
      record.put("temperature", value.get("temperature"));
      record.put("stationId", value.get("stationId"));
      return record;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
    AvroJob.setMapOutputSchema(conf,
        Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
    AvroJob.setOutputSchema(conf, SCHEMA);
    
    conf.setInputFormat(AvroUtf8InputFormat.class);

    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
    AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
    System.exit(exitCode);
  }
}
// ^^ AvroGenericMaxTemperature
