package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class JoinReducer extends MapReduceBase implements
    Reducer<TextPair, Text, Text, Text> {

  public void reduce(TextPair key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    Text stationName = new Text(values.next());
    while (values.hasNext()) {
      Text record = values.next();
      Text outValue = new Text(stationName.toString() + "\t" + record.toString());
      output.collect(key.getFirst(), outValue);
    }
  }
}
