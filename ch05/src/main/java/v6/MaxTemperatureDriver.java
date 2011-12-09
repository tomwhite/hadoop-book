package v6;
//== MaxTemperatureDriverV6

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;
import v5.MaxTemperatureMapper;

//Identical to v5 except for profiling configuration
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    //vv MaxTemperatureDriverV6
    Configuration conf = getConf();
    conf.setBoolean("mapred.task.profile", true);
    conf.set("mapred.task.profile.params", "-agentlib:hprof=cpu=samples," +
        "heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
    conf.set("mapred.task.profile.maps", "0-2");
    conf.set("mapred.task.profile.reduces", ""); // no reduces
    Job job = new Job(conf, "Max temperature");
    //^^ MaxTemperatureDriverV6
    
    // Following alternative is only available in 0.21 onwards
//    conf.setBoolean(JobContext.TASK_PROFILE, true);
//    conf.set(JobContext.TASK_PROFILE_PARAMS, "-agentlib:hprof=cpu=samples," +
//        "heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
//    conf.set(JobContext.NUM_MAP_PROFILES, "0-2");
//    conf.set(JobContext.NUM_REDUCE_PROFILES, "");
    
    job.setJarByClass(getClass());
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

