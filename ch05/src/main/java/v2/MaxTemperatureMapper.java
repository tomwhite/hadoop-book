package v2;
//== MaxTemperatureMapperV2

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
//vv MaxTemperatureMapperV2
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    /*[*/String temp = line.substring(87, 92);
    if (!missing(temp)) {/*]*/
        int airTemperature = Integer.parseInt(temp);
        output.collect(new Text(year), new IntWritable(airTemperature));
    /*[*/}/*]*/
  }
  
  /*[*/private boolean missing(String temp) {
    return temp.equals("+9999");
  }/*]*/
//^^ MaxTemperatureMapperV2
}
