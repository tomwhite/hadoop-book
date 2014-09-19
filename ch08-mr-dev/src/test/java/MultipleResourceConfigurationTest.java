// == MultipleResourceConfigurationTest
// == MultipleResourceConfigurationTest-Override
// == MultipleResourceConfigurationTest-Final
// == MultipleResourceConfigurationTest-Expansion
// == MultipleResourceConfigurationTest-SystemExpansion
// == MultipleResourceConfigurationTest-NoSystemByDefault
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class MultipleResourceConfigurationTest {
  
  @Test
  public void get() throws IOException {
    // Single test as an expedient for inclusion in the book
    
    // vv MultipleResourceConfigurationTest
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    conf.addResource("configuration-2.xml");
    // ^^ MultipleResourceConfigurationTest
    
    assertThat(conf.get("color"), is("yellow"));

    // override
    // vv MultipleResourceConfigurationTest-Override
    assertThat(conf.getInt("size", 0), is(12));
    // ^^ MultipleResourceConfigurationTest-Override

    // final properties cannot be overridden
    // vv MultipleResourceConfigurationTest-Final
    assertThat(conf.get("weight"), is("heavy"));
    // ^^ MultipleResourceConfigurationTest-Final

    // variable expansion
    // vv MultipleResourceConfigurationTest-Expansion
    assertThat(conf.get("size-weight"), is("12,heavy"));
    // ^^ MultipleResourceConfigurationTest-Expansion

    // variable expansion with system properties
    // vv MultipleResourceConfigurationTest-SystemExpansion
    System.setProperty("size", "14");
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // ^^ MultipleResourceConfigurationTest-SystemExpansion

    // system properties are not picked up
    // vv MultipleResourceConfigurationTest-NoSystemByDefault
    System.setProperty("length", "2");
    assertThat(conf.get("length"), is((String) null));
    // ^^ MultipleResourceConfigurationTest-NoSystemByDefault

  }

}
