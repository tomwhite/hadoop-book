import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.Test;

public class LineReaderTest {
  @Test
  public void linesEndWithTerminators() throws Exception {
    
    String input = "line1\nline2\r\nline3";
    LineReader lineReader = new LineReader(new ByteArrayInputStream(input.getBytes()));
    
    Text line = new Text();
    
    lineReader.readLine(line);
    assertThat(line.toString(), is("line1"));
    
    lineReader.readLine(line);
    assertThat(line.toString(), is("line2"));

    lineReader.readLine(line);
    assertThat(line.toString(), is("line3"));
    
  }
}
