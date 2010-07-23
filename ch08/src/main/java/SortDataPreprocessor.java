// cc SortDataPreprocessor A MapReduce program for transforming the weather data into SequenceFile format
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// vv SortDataPreprocessor
public class SortDataPreprocessor extends Configured implements Tool {
  
  static class CleanerMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }

    conf.setMapperClass(CleanerMapper.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(0);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);
  }
}
// ^^ SortDataPreprocessor