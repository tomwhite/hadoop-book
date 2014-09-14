package v2;
// cc MaxTemperatureMapperV2 A Mapper that uses a utility class to parse records

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import v2.NcdcRecordParser;

// vv MaxTemperatureMapperV2
public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*[*/private NcdcRecordParser parser = new NcdcRecordParser();/*]*/

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*[*/parser.parse(value);/*]*/
    if (/*[*/parser.isValidTemperature()/*]*/) {
      context.write(new Text(/*[*/parser.getYear()/*]*/),
          new IntWritable(/*[*/parser.getAirTemperature()/*]*/));
    }
  }
}
// ^^ MaxTemperatureMapperV2
