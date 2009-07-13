// == StationPartitioner
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv StationPartitioner
public class StationPartitioner implements Partitioner<LongWritable, Text> {
  
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  public int getPartition(LongWritable key, Text value, int numPartitions) {
    parser.parse(value);
    return getPartition(parser.getStationId());
  }

  private int getPartition(String stationId) {
    /*...*/
// ^^ StationPartitioner
    return 0;
// vv StationPartitioner
  }

  @Override
  public void configure(JobConf conf) { }
}
// ^^ StationPartitioner
