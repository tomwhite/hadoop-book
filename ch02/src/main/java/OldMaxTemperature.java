// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv OldMaxTemperature
public class OldMaxTemperature {
  
  static class OldMaxTemperatureMapper /*[*/extends MapReduceBase/*]*/
    /*[*/implements Mapper/*]*/<LongWritable, Text, Text, IntWritable> {
  
    private static final int MISSING = 9999;
    
    @Override
    public void map(LongWritable key, Text value,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
        throws IOException {
      
      String line = value.toString();
      String year = line.substring(15, 19);
      int airTemperature;
      if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
        airTemperature = Integer.parseInt(line.substring(88, 92));
      } else {
        airTemperature = Integer.parseInt(line.substring(87, 92));
      }
      String quality = line.substring(92, 93);
      if (airTemperature != MISSING && quality.matches("[01459]")) {
        /*[*/output.collect/*]*/(new Text(year), new IntWritable(airTemperature));
      }
    }
  }
  
  static class OldMaxTemperatureReducer /*[*/extends MapReduceBase/*]*/
    /*[*/implements Reducer/*]*/<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, /*[*/Iterator/*]*/<IntWritable> values,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
        throws IOException {
      
      int maxValue = Integer.MIN_VALUE;
      while (/*[*/values.hasNext()/*]*/) {
        maxValue = Math.max(maxValue, /*[*/values.next().get()/*]*/);
      }
      /*[*/output.collect/*]*/(key, new IntWritable(maxValue));
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: OldMaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    /*[*/JobConf conf = new JobConf(OldMaxTemperature.class);/*]*/
    /*[*/conf/*]*/.setJobName("Max temperature");

    FileInputFormat.addInputPath(/*[*/conf/*]*/, new Path(args[0]));
    FileOutputFormat.setOutputPath(/*[*/conf/*]*/, new Path(args[1]));
    
    /*[*/conf/*]*/.setMapperClass(OldMaxTemperatureMapper.class);
    /*[*/conf/*]*/.setReducerClass(OldMaxTemperatureReducer.class);

    /*[*/conf/*]*/.setOutputKeyClass(Text.class);
    /*[*/conf/*]*/.setOutputValueClass(IntWritable.class);

    /*[*/JobClient.runJob(conf);/*]*/
  }
}
// ^^ OldMaxTemperature