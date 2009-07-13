import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.*;

public class HBaseTemperatureCli extends Configured implements Tool {
  
  public NavigableMap<Long, Integer> getStationObservations(HTable table,
      String stationId, long maxStamp, int maxCount) throws IOException {
    byte[][] columns = { Bytes.toBytes("data:airtemp") };
    byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);
    RowResult res = null;
    NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();
    byte[] airtempColumn = Bytes.toBytes("data:airtemp");
    Scanner s = table.getScanner(columns, startRow);
    int count = 0;
    try {
      while ((res = s.next()) != null && count++ < maxCount) {
        byte[] row = res.getRow();
        byte[] value = res.get(airtempColumn).getValue();
        Long stamp = Long.MAX_VALUE -
          Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        Integer temp = Bytes.toInt(value);
        resultMap.put(stamp, temp);
      }
    } finally {
      s.close();
    }
    return resultMap;
  }

  /**
   * Return the last ten observations.
   */
  public NavigableMap<Long, Integer> getStationObservations(HTable table,
      String stationId) throws IOException {
    return getStationObservations(table, stationId, Long.MAX_VALUE, 10);
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureCli <station_id>");
      return -1;
    }
    
    HTable table = new HTable(new HBaseConfiguration(getConf()), "observations");
    NavigableMap<Long, Integer> observations =
      getStationObservations(table, args[0]).descendingMap();
    for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
      // Print the date, time, and temperature
      System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
          observation.getValue());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseTemperatureCli(), args);
    System.exit(exitCode);
  }
}
