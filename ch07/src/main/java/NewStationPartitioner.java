import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NewStationPartitioner extends Partitioner<LongWritable, Text> {
  
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  public int getPartition(LongWritable key, Text value, int numPartitions) {
    parser.parse(value);
    return getPartition(parser.getStationId());
  }

  private int getPartition(String stationId) {
    /*...*/
    return 0;
  }

}
