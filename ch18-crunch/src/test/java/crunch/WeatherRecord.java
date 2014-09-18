package crunch;

public class WeatherRecord {
  private int year;
  private int temperature;
  private String stationId;

  public WeatherRecord() {
  }

  public WeatherRecord(int year, int temperature, String stationId) {
    this.year = year;
    this.temperature = temperature;
    this.stationId = stationId;
  }

  // ... getters elided

  public int getYear() {
    return year;
  }

  public int getTemperature() {
    return temperature;
  }

  public String getStationId() {
    return stationId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WeatherRecord that = (WeatherRecord) o;

    if (temperature != that.temperature) return false;
    if (year != that.year) return false;
    if (stationId != null ? !stationId.equals(that.stationId) : that.stationId != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = year;
    result = 31 * result + temperature;
    result = 31 * result + (stationId != null ? stationId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "WeatherRecord{" +
        "year=" + year +
        ", temperature=" + temperature +
        ", stationId='" + stationId + '\'' +
        '}';
  }
}
