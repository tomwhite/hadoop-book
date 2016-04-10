import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0 version of HBaseStationImporter that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseStationImporter extends Configured implements Tool {
  
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }

    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      // Create table
      TableName tableName = TableName.valueOf("stations");
      Table table = connection.getTable(tableName);
      try {
        NcdcStationMetadata metadata = new NcdcStationMetadata();
        metadata.initialize(new File(args[0]));
        Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

        for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
          Put put = new Put(Bytes.toBytes(entry.getKey()));
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.DESCRIPTION_QUALIFIER, Bytes.toBytes("(unknown)"));
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.LOCATION_QUALIFIER, Bytes.toBytes("(unknown)"));
          table.put(put);
        }
      } finally {
        table.close();
      }
    } finally {
      connection.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseStationImporter(), args);
    System.exit(exitCode);
  }
}