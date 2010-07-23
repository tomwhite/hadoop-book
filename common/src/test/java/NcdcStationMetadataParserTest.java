import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

public class NcdcStationMetadataParserTest {
  
  private NcdcStationMetadataParser parser;

  @Before
  public void setUp() {
    parser = new NcdcStationMetadataParser();
  }
  
  @Test
  public void parsesValidRecord() {
    assertThat(parser.parse("715390 99999 MOOSE JAW CS                  CN CA SA CZMJ  +50317 -105550 +05770"), is(true));
    assertThat(parser.getStationId(), is("715390-99999"));
    assertThat(parser.getStationName().trim(), is("MOOSE JAW CS"));
  }
  
  @Test
  public void parsesHeader() {
    assertThat(parser.parse("Integrated Surface Database Station History, November 2007"), is(false));
  }
  
  public void parsesBlankLine() {
    assertThat(parser.parse(""), is(false));
  }
  
}
