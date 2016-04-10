import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0 version of HBaseStationQuery that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseStationQuery extends Configured implements Tool {
  static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
  static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

  public Map<String, String> getStationInfo(Table table, String stationId)
      throws IOException {
    Get get = new Get(Bytes.toBytes(stationId));
    get.addFamily(INFO_COLUMNFAMILY);
    Result res = table.get(get);
    if (res == null) {
      return null;
    }
    Map<String, String> resultMap = new LinkedHashMap<String, String>();
    resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
    resultMap.put("location", getValue(res, INFO_COLUMNFAMILY,
        LOCATION_QUALIFIER));
    resultMap.put("description", getValue(res, INFO_COLUMNFAMILY,
        DESCRIPTION_QUALIFIER));
    return resultMap;
  }

  private static String getValue(Result res, byte[] cf, byte[] qualifier) {
    byte[] value = res.getValue(cf, qualifier);
    return value == null? "": Bytes.toString(value);
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationQuery <station_id>");
      return -1;
    }

    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      TableName tableName = TableName.valueOf("stations");
      Table table = connection.getTable(tableName);
      try {
        Map<String, String> stationInfo = getStationInfo(table, args[0]);
        if (stationInfo == null) {
          System.err.printf("Station ID %s not found.\n", args[0]);
          return -1;
        }
        for (Map.Entry<String, String> station : stationInfo.entrySet()) {
          System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
        }
        return 0;
      } finally {
        table.close();
      }
    } finally {
      connection.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseStationQuery(), args);
    System.exit(exitCode);
  }
}