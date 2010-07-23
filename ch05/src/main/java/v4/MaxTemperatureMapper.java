// == MaxTemperatureMapperV4
package v4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import v3.NcdcRecordParser;

//vv MaxTemperatureMapperV4
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  /*[*/enum Temperature {
    OVER_100
  }/*]*/
  
  private NcdcRecordParser parser = new NcdcRecordParser();

  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    parser.parse(value);
    if (parser.isValidTemperature()) {
      int airTemperature = parser.getAirTemperature();
      /*[*/if (airTemperature > 1000) {
        System.err.println("Temperature over 100 degrees for input: " + value);
        reporter.setStatus("Detected possibly corrupt record: see logs.");
        reporter.incrCounter(Temperature.OVER_100, 1);
      }/*]*/
      output.collect(new Text(parser.getYear()), new IntWritable(airTemperature));
    }
  }
}
//^^ MaxTemperatureMapperV4

