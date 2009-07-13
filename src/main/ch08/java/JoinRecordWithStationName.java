// cc JoinRecordWithStationName Application to join weather records with station names
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

// vv JoinRecordWithStationName
public class JoinRecordWithStationName extends Configured implements Tool {
  
  public static class KeyPartitioner implements Partitioner<TextPair, Text> {
    @Override
    public void configure(JobConf job) {}
    
    @Override
    public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
      return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Join record with station name");
    
    Path ncdcInputPath = new Path(args[0]);
    Path stationInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    MultipleInputs.addInputPath(conf, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    MultipleInputs.addInputPath(conf, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    FileOutputFormat.setOutputPath(conf, outputPath);

    /*[*/conf.setPartitionerClass(KeyPartitioner.class);
    conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);/*]*/
    
    conf.setMapOutputKeyClass(TextPair.class);
    
    conf.setReducerClass(JoinReducer.class);

    conf.setOutputKeyClass(Text.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
    System.exit(exitCode);
  }
}
// ^^ JoinRecordWithStationName
