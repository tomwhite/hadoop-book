import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

public class MetOfficeRecordParserTest {
  
  private MetOfficeRecordParser parser;

  @Before
  public void setUp() {
    parser = new MetOfficeRecordParser();
  }
  
  @Test
  public void parsesValidRecord() {
    parser.parse("   1978   1    7.5     2.0       6   134.1    64.7");
    assertThat(parser.getYear(), is("1978"));
    assertThat(parser.getAirTemperature(), is(75));
    assertThat(parser.getAirTemperatureString(), is("  7.5"));
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  @Test
  public void parsesNegativeTemperature() {
    parser.parse("   1978   1  -17.5     2.0       6   134.1    64.7");
    assertThat(parser.getYear(), is("1978"));
    assertThat(parser.getAirTemperature(), is(-175));
    assertThat(parser.getAirTemperatureString(), is("-17.5"));
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  @Test
  public void parsesMissingTemperature() {
    parser.parse("   1853   1    ---     ---     ---    57.3     ---");
    assertThat(parser.getAirTemperatureString(), is("  ---"));
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  @Test
  public void parsesHeaderLine() {
    parser.parse("Cardiff Bute Park");
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    parser.parse("   1978   1    X.5     2.0       6   134.1    64.7");
  }
}
