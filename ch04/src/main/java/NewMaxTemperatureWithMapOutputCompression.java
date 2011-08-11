import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewMaxTemperatureWithMapOutputCompression {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: NewMaxTemperatureWithMapOutputCompression <input path> " +
        "<output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.map.output.compress", true);
    conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class,
        CompressionCodec.class);
    
    Job job = new Job(conf);
    job.setJarByClass(NewMaxTemperature.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(NewMaxTemperature.NewMaxTemperatureMapper.class);
    job.setCombinerClass(NewMaxTemperature.NewMaxTemperatureReducer.class);
    job.setReducerClass(NewMaxTemperature.NewMaxTemperatureReducer.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
