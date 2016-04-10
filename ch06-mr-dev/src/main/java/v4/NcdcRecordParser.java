package v4;
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private String year;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    if (record.charAt(87) == '+') { 
      airTemperature = Integer.parseInt(record.substring(88, 92));
    } else if (record.charAt(87) == '-') {
      airTemperature = Integer.parseInt(record.substring(87, 92));
    } else {
      airTemperatureMalformed = true;
    }
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }

  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
}
