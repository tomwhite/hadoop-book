import java.math.*;
import org.apache.hadoop.io.Text;

public class MetOfficeRecordParser {
  
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureValid;
  
  public void parse(String record) {
    if (record.length() < 18) {
      return;
    }
    year = record.substring(3, 7);
    if (isValidRecord(year)) {
      airTemperatureString = record.substring(13, 18);
      if (!airTemperatureString.trim().equals("---")) {
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        airTemperature = temp.intValueExact();
        airTemperatureValid = true;
      }
    }
  }
  
  private boolean isValidRecord(String year) {
    try {
      Integer.parseInt(year);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public void parse(Text record) {
    parse(record.toString());
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}
