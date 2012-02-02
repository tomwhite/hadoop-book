package oldapi;

import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  private String stationId;
  private String observationDateString;
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    observationDateString = record.substring(15, 27);
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') {
      airTemperatureString = record.substring(88, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else if (record.charAt(87) == '-') {
      airTemperatureString = record.substring(87, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
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
  
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  public String getStationId() {
    return stationId;
  }
  
  public Date getObservationDate() {
    try {
      System.out.println(observationDateString);
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getYear() {
    return year;
  }

  public int getYearInt() {
    return Integer.parseInt(year);
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public String getQuality() {
    return quality;
  }

}
