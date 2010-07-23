// cc NewMaxTemperature Application to find the maximum temperature in the weather dataset using the new context objects MapReduce API
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv NewMaxTemperature
public class NewMaxTemperature {
  
  static class NewMaxTemperatureMapper
    /*[*/extends Mapper<LongWritable, Text, Text, IntWritable>/*]*/ {

    private static final int MISSING = 9999;
    
    public void map(LongWritable key, Text value, /*[*/Context context/*]*/)
        throws IOException, /*[*/InterruptedException/*]*/ {
      
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
        /*[*/context.write/*]*/(new Text(year), new IntWritable(airTemperature));
      }
    }
  }
  
  static class NewMaxTemperatureReducer
    /*[*/extends Reducer<Text, IntWritable, Text, IntWritable>/*]*/ {
  
    public void reduce(Text key, /*[*/Iterable/*]*/<IntWritable> values,
        /*[*/Context context/*]*/)
        throws IOException, /*[*/InterruptedException/*]*/ {
      
      int maxValue = Integer.MIN_VALUE;
      for (IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
      }
      /*[*/context.write/*]*/(key, new IntWritable(maxValue));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: NewMaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    /*[*/Job job = new Job();
    job.setJarByClass(NewMaxTemperature.class);/*]*/

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(NewMaxTemperatureMapper.class);
    job.setReducerClass(NewMaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    /*[*/System.exit(job.waitForCompletion(true) ? 0 : 1);/*]*/
  }
}
// ^^ NewMaxTemperature
