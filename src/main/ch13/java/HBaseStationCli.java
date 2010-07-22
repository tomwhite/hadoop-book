import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.*;

public class HBaseStationCli extends Configured implements Tool {

  public Map<String, String> getStationInfo(HTable table, String stationId)
      throws IOException {
    byte[][] columns = { Bytes.toBytes("info:") };
    RowResult res = table.getRow(Bytes.toBytes(stationId), columns);
    if (res == null) {
      return null;
    }
    Map<String, String> resultMap = new HashMap<String, String>();
    resultMap.put("name", getValue(res, "info:name"));
    resultMap.put("location", getValue(res, "info:location"));
    resultMap.put("description", getValue(res, "info:description"));
    return resultMap;
  }

  private static String getValue(RowResult res, String key) {
    Cell c = res.get(key.getBytes());
    if (c == null) {
      return "";
    }
    return Bytes.toString(c.getValue());
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationCli <station_id>");
      return -1;
    }

    HTable table = new HTable(new HBaseConfiguration(getConf()), "stations");
    Map<String, String> stationInfo = getStationInfo(table, args[0]);
    if (stationInfo == null) {
      System.err.printf("Station ID %s not found.\n", args[0]);
      return -1;
    }
    for (Map.Entry<String, String> station : stationInfo.entrySet()) {
      // Print the date, time, and temperature
      System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseStationCli(), args);
    System.exit(exitCode);
  }
}
