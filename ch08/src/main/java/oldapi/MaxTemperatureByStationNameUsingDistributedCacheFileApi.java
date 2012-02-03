// == OldMaxTemperatureByStationNameUsingDistributedCacheFileApi
package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemperatureByStationNameUsingDistributedCacheFileApi
  extends Configured implements Tool {
  
  static class StationTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    private NcdcStationMetadata metadata;
    
    // vv OldMaxTemperatureByStationNameUsingDistributedCacheFileApi
    @Override
    public void configure(JobConf conf) {
      metadata = new NcdcStationMetadata();
      try {
        Path[] localPaths = /*[*/DistributedCache.getLocalCacheFiles(conf);/*]*/
        if (localPaths.length == 0) {
          throw new FileNotFoundException("Distributed cache file not found.");
        }
        File localFile = new File(localPaths[0].toString());
        metadata.initialize(localFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // ^^ OldMaxTemperatureByStationNameUsingDistributedCacheFileApi

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      String stationName = metadata.getStationName(key.toString());
      
      int maxValue = Integer.MIN_VALUE;
      while (values.hasNext()) {
        maxValue = Math.max(maxValue, values.next().get());
      }
      output.collect(new Text(stationName), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(StationTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFileApi(), args);
    System.exit(exitCode);
  }
}
