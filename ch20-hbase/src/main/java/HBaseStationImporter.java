import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseStationImporter extends Configured implements Tool {
  
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }
    
    HTable table = new HTable(HBaseConfiguration.create(getConf()), "stations");
    try {
      NcdcStationMetadata metadata = new NcdcStationMetadata();
      metadata.initialize(new File(args[0]));
      Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

      for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
        Put put = new Put(Bytes.toBytes(entry.getKey()));
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.NAME_QUALIFIER,
            Bytes.toBytes(entry.getValue()));
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.DESCRIPTION_QUALIFIER,

            Bytes.toBytes("(unknown)"));
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.LOCATION_QUALIFIER,
            Bytes.toBytes("(unknown)"));
        table.put(put);
      }
      return 0;
    } finally {
      table.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseStationImporter(), args);
    System.exit(exitCode);
  }
}