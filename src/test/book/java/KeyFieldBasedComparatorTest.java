import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.junit.Test;


public class KeyFieldBasedComparatorTest {

  Text line1 = new Text("2\t30");
  Text line2 = new Text("10\t4");
  Text line3 = new Text("10\t30");
  
  @Test
  public void firstKey() throws Exception {
    check("-k1,1", line1, line2, 1);
    check("-k1",   line1, line2, 1);
    check("-k1.1", line1, line2, 1);
    check("-k1n",  line1, line2, -1);
    check("-k1nr", line1, line2, 1);
  }

  @Test
  public void secondKey() throws Exception {
    check("-k2,2", line1, line2, -1);
    check("-k2",   line1, line2, -1);
    check("-k2.1", line1, line2, -1);
    check("-k2n",  line1, line2, 1);
    check("-k2nr", line1, line2, -1);
  }

  @Test
  public void firstThenSecondKey() throws Exception {
    check("-k1 -k2",   line1, line2, 1);
    check("-k1 -k2",   line2, line3, 1);
    //check("-k1 -k2n",  line2, line3, -1);
    check("-k1 -k2nr", line2, line3, 1);
  }
  
  private void check(String options, Text l1, Text l2, int c) throws IOException {
    JobConf conf = new JobConf();
    conf.setKeyFieldComparatorOptions(options);
    KeyFieldBasedComparator comp = new KeyFieldBasedComparator();
    comp.configure(conf);
    
    DataOutputBuffer out1 = serialize(l1);
    DataOutputBuffer out2 = serialize(l2);
    assertThat(options, comp.compare(out1.getData(), 0, out1.getLength(), out2.getData(), 0, out2.getLength()), is(c));
  }
  
  public static DataOutputBuffer serialize(Writable writable) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out;
  }
  
}
