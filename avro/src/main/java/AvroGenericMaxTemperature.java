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

public class AvroGenericMaxTemperature extends Configured implements Tool {
  
  private static final Schema SCHEMA = Schema.parse("{\"type\":\"record\", \"name\":\"WeatherRecord\", \"fields\":"
      + "[{\"name\":\"year\", \"type\":\"int\"}, " +
      "{\"name\":\"temperature\", \"type\":\"int\", \"order\": \"ignore\"}, " +
      "{\"name\":\"stationId\", \"type\":\"string\", \"order\": \"ignore\"}]}");
  
  private static GenericRecord newWeatherRecord(int year, int temperature, String stationId) {
    GenericRecord value = new GenericData.Record(SCHEMA);
    value.put("year", year);
    value.put("temperature", temperature);
    value.put("stationId", new Utf8(stationId));
    return value;
  }
  
  private static GenericRecord newWeatherRecord(GenericRecord other) {
    GenericRecord value = new GenericData.Record(SCHEMA);
    value.put("year", other.get("year"));
    value.put("temperature", other.get("temperature"));
    value.put("stationId", other.get("stationId"));
    return value;
  }
  
  public static class MaxTemperatureMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    @Override
    public void map(Utf8 line,
        AvroCollector<Pair<Integer, GenericRecord>> collector,
        Reporter reporter) throws IOException {
      parser.parse(line.toString());
      if (parser.isValidTemperature()) {
        GenericRecord record = newWeatherRecord(parser.getYearInt(), parser.getAirTemperature(), parser.getStationId());
        Pair<Integer, GenericRecord> pair =
          new Pair<Integer, GenericRecord>(parser.getYearInt(), record);
        collector.collect(pair);
      }
    }
  }
  
  public static class MaxTemperatureReducer extends
      AvroReducer<Integer, GenericRecord, GenericRecord> {

    @Override
    public void reduce(Integer key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
      GenericRecord max = null;
      for (GenericRecord value : values) {
        if (max == null) {
          max = newWeatherRecord(value);
        } else {
          int previousMax = (Integer) max.get("temperature");
          int currentMax = (Integer) value.get("temperature");
          if (currentMax > previousMax) {
            max = newWeatherRecord(value);
          }
        }
      }
      collector.collect(max);
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
