package v7;
// cc MaxTemperatureMapperV7 Reusing the Text and IntWritable output objects
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import v5.NcdcRecordParser;

// vv MaxTemperatureMapperV7
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
  enum Temperature {
    MALFORMED
  }

  private NcdcRecordParser parser = new NcdcRecordParser();
  /*[*/private Text year = new Text();
  private IntWritable temp = new IntWritable();/*]*/
  
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    parser.parse(value);
    if (parser.isValidTemperature()) {
      /*[*/year.set(parser.getYear());
      temp.set(parser.getAirTemperature());
      output.collect(year, temp);/*]*/
    } else if (parser.isMalformedTemperature()) {
      System.err.println("Ignoring possibly corrupt input: " + value);
      reporter.incrCounter(Temperature.MALFORMED, 1);
    }
  }
}
// ^^ MaxTemperatureMapperV7
