// cc MinimalMapReduceWithDefaults A minimal MapReduce driver, with the defaults explicitly set
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

//vv MinimalMapReduceWithDefaults
public class MinimalMapReduceWithDefaults extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    /*[*/conf.setInputFormat(TextInputFormat.class);
    
    conf.setNumMapTasks(1);
    conf.setMapperClass(IdentityMapper.class);
    conf.setMapRunnerClass(MapRunner.class);
    
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);
    
    conf.setPartitionerClass(HashPartitioner.class);
    
    conf.setNumReduceTasks(1);
    conf.setReducerClass(IdentityReducer.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);/*]*/
    
    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
    System.exit(exitCode);
  }
}
// ^^ MinimalMapReduceWithDefaults
