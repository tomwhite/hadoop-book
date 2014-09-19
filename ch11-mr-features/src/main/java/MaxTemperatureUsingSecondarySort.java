// cc MaxTemperatureUsingSecondarySort Application to find the maximum temperature by sorting temperatures in the key
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureUsingSecondarySort
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  static class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        /*[*/context.write(new IntPair(parser.getYearInt(),
            parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  static class MaxTemperatureReducer
    extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    @Override
    protected void reduce(IntPair key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
      
      /*[*/context.write(key, NullWritable.get());/*]*/
    }
  }
  
  public static class FirstPartitioner
    extends Partitioner<IntPair, NullWritable> {

    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
      // multiply by 127 to perform some mixing
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      return IntPair.compare(ip1.getFirst(), ip2.getFirst());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setPartitionerClass(FirstPartitioner.class);/*]*/
    /*[*/job.setSortComparatorClass(KeyComparator.class);/*]*/
    /*[*/job.setGroupingComparatorClass(GroupComparator.class);/*]*/
    job.setReducerClass(MaxTemperatureReducer.class);
    job.setOutputKeyClass(IntPair.class);
    job.setOutputValueClass(NullWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureUsingSecondarySort
