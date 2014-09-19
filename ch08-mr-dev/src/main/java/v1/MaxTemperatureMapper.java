package v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature = Integer.parseInt(line.substring(87, 92));
    context.write(new Text(year), new IntWritable(airTemperature));
  }
}
//^^ MaxTemperatureMapperV1
