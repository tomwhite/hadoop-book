import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.*;

public class TemperatureDistribution extends Configured implements Tool {
  
  static class TemperatureCountMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, LongWritable> {
  
    private static final LongWritable ONE = new LongWritable(1);
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new IntWritable(parser.getAirTemperature() / 10), ONE);
      }
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(TemperatureCountMapper.class);
    conf.setCombinerClass(LongSumReducer.class);
    conf.setReducerClass(LongSumReducer.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(LongWritable.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
    System.exit(exitCode);
  }
}
