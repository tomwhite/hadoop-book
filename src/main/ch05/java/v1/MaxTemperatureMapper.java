package v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature = Integer.parseInt(line.substring(87, 92));
    output.collect(new Text(year), new IntWritable(airTemperature));
  }
}
//^^ MaxTemperatureMapperV1
