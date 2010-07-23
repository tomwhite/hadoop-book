// cc LookupRecordsByTemperature Retrieve all entries with a given key from a collection of MapFiles
// == LookupRecordsByTemperature-ReaderFragment
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.*;

// vv LookupRecordsByTemperature
public class LookupRecordsByTemperature extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      JobBuilder.printUsage(this, "<path> <key>");
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    FileSystem fs = path.getFileSystem(getConf());
    
    Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    
    // vv LookupRecordsByTemperature-ReaderFragment
    Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
    // ^^ LookupRecordsByTemperature-ReaderFragment
    Writable entry = reader.get(key, val);
    if (entry == null) {
      System.err.println("Key not found: " + key);
      return -1;
    }
    NcdcRecordParser parser = new NcdcRecordParser();
    IntWritable nextKey = new IntWritable();
    do {
      parser.parse(val.toString());
      System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    } while(reader.next(nextKey, val) && key.equals(nextKey));
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
    System.exit(exitCode);
  }
}
// ^^ LookupRecordsByTemperature
