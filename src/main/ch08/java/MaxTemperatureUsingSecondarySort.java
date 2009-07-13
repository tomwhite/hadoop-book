// cc MaxTemperatureUsingSecondarySort Application to find the maximum temperature by sorting temperatures in the key
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// vv MaxTemperatureUsingSecondarySort
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  static class MaxTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        /*[*/output.collect(new IntPair(parser.getYearInt(),
            + parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  static class MaxTemperatureReducer extends MapReduceBase
    implements Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    public void reduce(IntPair key, Iterator<NullWritable> values,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      /*[*/output.collect(key, NullWritable.get());/*]*/
    }
  }
  
  public static class FirstPartitioner
    implements Partitioner<IntPair, NullWritable> {

    @Override
    public void configure(JobConf job) {}
    
    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
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
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    /*[*/conf.setPartitionerClass(FirstPartitioner.class);/*]*/
    /*[*/conf.setOutputKeyComparatorClass(KeyComparator.class);/*]*/
    /*[*/conf.setOutputValueGroupingComparator(GroupComparator.class);/*]*/
    conf.setReducerClass(MaxTemperatureReducer.class);
    conf.setOutputKeyClass(IntPair.class);
    conf.setOutputValueClass(NullWritable.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureUsingSecondarySort
