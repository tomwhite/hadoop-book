import java.io.*;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.*;

public class HBaseStationImporter extends Configured implements Tool {
  
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }
    
    HTable table = new HTable(new HBaseConfiguration(getConf()), "stations");
    
    NcdcStationMetadata metadata = new NcdcStationMetadata();
    metadata.initialize(new File(args[0]));
    Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();
    
    for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
      byte[] rowKey = Bytes.toBytes(entry.getKey());
      BatchUpdate bu = new BatchUpdate(rowKey);
      bu.put("info:name", Bytes.toBytes(entry.getValue()));
      bu.put("info:description", Bytes.toBytes("Description..."));
      bu.put("info:location", Bytes.toBytes("Location..."));
      table.commit(bu);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseStationImporter(), args);
    System.exit(exitCode);
  }
}