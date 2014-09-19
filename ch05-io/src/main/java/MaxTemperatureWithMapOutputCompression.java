// == MaxTemperatureWithMapOutputCompression
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureWithMapOutputCompression {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
        "<input path> <output path>");
      System.exit(-1);
    }

    // vv MaxTemperatureWithMapOutputCompression
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
        CompressionCodec.class);
    Job job = new Job(conf);
    // ^^ MaxTemperatureWithMapOutputCompression
    job.setJarByClass(MaxTemperature.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
