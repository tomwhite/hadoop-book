// == MaxTemperatureWithMultipleInputFormats
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class MaxTemperatureWithMultipleInputFormats extends Configured
  implements Tool {
  
  static class MetOfficeMaxTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
  
    private MetOfficeRecordParser parser = new MetOfficeRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new Text(parser.getYear()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <metoffice input> <output>");
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature with multiple input formats");
    
    Path ncdcInputPath = new Path(args[0]);
    Path metOfficeInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
// vv MaxTemperatureWithMultipleInputFormats    
    MultipleInputs.addInputPath(conf, ncdcInputPath,
        TextInputFormat.class, MaxTemperatureMapper.class);
    MultipleInputs.addInputPath(conf, metOfficeInputPath,
        TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
// ^^ MaxTemperatureWithMultipleInputFormats
    FileOutputFormat.setOutputPath(conf, outputPath);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputFormats(),
        args);
    System.exit(exitCode);
  }
}
