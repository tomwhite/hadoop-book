package crunch;

import java.io.Serializable;
import org.apache.hadoop.io.Text;

// Serializable copy of NcdcStationMetadataParser
public class NcdcStationMetadataParser implements Serializable {
  
  private String stationId;
  private String stationName;

  public boolean parse(String record) {
    if (record.length() < 42) { // header
      return false;
    }
    String usaf = record.substring(0, 6);
    String wban = record.substring(7, 12);
    stationId = usaf + "-" + wban;
    stationName = record.substring(13, 42);
    try {
      Integer.parseInt(usaf); // USAF identifiers are numeric
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  public boolean parse(Text record) {
    return parse(record.toString());
  }
  
  public String getStationId() {
    return stationId;
  }

  public String getStationName() {
    return stationName;
  }
  
}
