// cc MaxTemperatureByStationNameUsingDistributedCacheFile Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file
import java.io.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

// vv MaxTemperatureByStationNameUsingDistributedCacheFile
public class NewMaxTemperatureByStationNameUsingDistributedCacheFile
  extends Configured implements Tool {
  
  static class StationTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    /*[*/private NcdcStationMetadata metadata;/*]*/
    
    /*[*/@Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      metadata = new NcdcStationMetadata();
      try {
        metadata.initialize(new File("stations-fixed-width.txt"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }/*]*/

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      
      /*[*/String stationName = metadata.getStationName(key.toString());/*]*/
      
      int maxValue = Integer.MIN_VALUE;
      for (IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
      }
      context.write(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // TODO: use JobBuilder
    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(StationTemperatureMapper.class);
    job.setCombinerClass(NewMaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new NewMaxTemperatureByStationNameUsingDistributedCacheFile(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureByStationNameUsingDistributedCacheFile
