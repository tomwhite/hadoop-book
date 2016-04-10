// == TextTest
// == TextTest-Find
// == TextTest-Mutability
// == TextTest-ByteArrayNotShortened
// == TextTest-ToString
// == TextTest-Comparison
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class TextTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv TextTest
    Text t = new Text("hadoop");
    assertThat(t.getLength(), is(6));
    assertThat(t.getBytes().length, is(6));
    
    assertThat(t.charAt(2), is((int) 'd'));
    assertThat("Out of bounds", t.charAt(100), is(-1));
    // ^^ TextTest
  }    

  @Test
  public void find() throws IOException {
    // vv TextTest-Find
    Text t = new Text("hadoop");
    assertThat("Find a substring", t.find("do"), is(2));
    assertThat("Finds first 'o'", t.find("o"), is(3));
    assertThat("Finds 'o' from position 4 or later", t.find("o", 4), is(4));
    assertThat("No match", t.find("pig"), is(-1));
    // ^^ TextTest-Find
  }
  
  @Test
  public void mutability() throws IOException {
    // vv TextTest-Mutability
    Text t = new Text("hadoop");
    t.set("pig");
    assertThat(t.getLength(), is(3));
    assertThat(t.getBytes().length, is(3));
    // ^^ TextTest-Mutability
  }
  
  @Test
  public void byteArrayNotShortened() throws IOException {
    // vv TextTest-ByteArrayNotShortened
    Text t = new Text("hadoop");
    t.set(/*[*/new Text("pig")/*]*/);
    assertThat(t.getLength(), is(3));
    assertThat("Byte length not shortened", t.getBytes().length,
      /*[*/is(6)/*]*/);
    // ^^ TextTest-ByteArrayNotShortened
  }

  @Test
  public void toStringMethod() throws IOException {
    // vv TextTest-ToString
    assertThat(new Text("hadoop").toString(), is("hadoop"));
    // ^^ TextTest-ToString
  }
  
  @Test
  public void comparison() throws IOException {
    assertThat("\ud800\udc00".compareTo("\ue000"), lessThan(0));
    assertThat(new Text("\ud800\udc00").compareTo(new Text("\ue000")), greaterThan(0));
  }
  
  @Test
  public void withSupplementaryCharacters() throws IOException {
    
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    assertThat(s.length(), is(5));
    assertThat(s.getBytes("UTF-8").length, is(10));
    
    assertThat(s.indexOf('\u0041'), is(0));
    assertThat(s.indexOf('\u00DF'), is(1));
    assertThat(s.indexOf('\u6771'), is(2));
    assertThat(s.indexOf('\uD801'), is(3));
    assertThat(s.indexOf('\uDC00'), is(4));
    
    assertThat(s.charAt(0), is('\u0041'));
    assertThat(s.charAt(1), is('\u00DF'));
    assertThat(s.charAt(2), is('\u6771'));
    assertThat(s.charAt(3), is('\uD801'));
    assertThat(s.charAt(4), is('\uDC00'));
    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    
    assertThat(serializeToString(t), is("0a41c39fe69db1f0909080"));
    
    assertThat(t.charAt(t.find("\u0041")), is(0x0041));
    assertThat(t.charAt(t.find("\u00DF")), is(0x00DF));
    assertThat(t.charAt(t.find("\u6771")), is(0x6771));
    assertThat(t.charAt(t.find("\uD801\uDC00")), is(0x10400));
    
  }    

}
