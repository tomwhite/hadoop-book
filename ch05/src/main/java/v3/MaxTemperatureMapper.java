package v3;
// cc MaxTemperatureMapperV3 A Mapper that uses a utility class to parse records

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv MaxTemperatureMapperV3
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
  /*[*/private NcdcRecordParser parser = new NcdcRecordParser();/*]*/
  
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    /*[*/parser.parse(value);/*]*/
    if (/*[*/parser.isValidTemperature()/*]*/) {
      output.collect(new Text(/*[*/parser.getYear()/*]*/),
          new IntWritable(/*[*/parser.getAirTemperature()/*]*/));
    }
  }
}
// ^^ MaxTemperatureMapperV3
