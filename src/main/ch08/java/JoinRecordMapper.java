// cc JoinRecordMapper Mapper for tagging weather records for a reduce-side join

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//vv JoinRecordMapper
public class JoinRecordMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  public void map(LongWritable key, Text value,
      OutputCollector<TextPair, Text> output, Reporter reporter)
      throws IOException {
    
    parser.parse(value);
    output.collect(new TextPair(parser.getStationId(), "1"), value);
  }
}
// ^^ JoinRecordMapper