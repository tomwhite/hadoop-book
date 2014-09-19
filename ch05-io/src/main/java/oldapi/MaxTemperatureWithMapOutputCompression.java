// == OldMaxTemperatureWithMapOutputCompression
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperatureWithMapOutputCompression {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
      		"<input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    conf.setJobName("Max temperature with map output compression");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    // vv OldMaxTemperatureWithMapOutputCompression
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
    // ^^ OldMaxTemperatureWithMapOutputCompression

    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
  }

}
