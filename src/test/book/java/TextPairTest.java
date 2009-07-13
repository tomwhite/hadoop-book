import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;


public class TextPairTest extends WritableTestBase {
  
  private TextPair tp1 = new TextPair("a", "b");
  private TextPair tp2 = new TextPair("b", "a");
  private TextPair tp3 = new TextPair("a", "ab");
  private TextPair tp4 = new TextPair("aa", "b");
  private TextPair tp5 = new TextPair(nTimes("a", 128), "b");
  private TextPair tp6 = new TextPair(nTimes("a", 128), nTimes("a", 128));
  private TextPair tp7 = new TextPair(nTimes("a", 128), nTimes("b", 128));
  
  private static String nTimes(String s, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
  
  @Test
  public void testComparator() throws IOException {
    check(tp1, tp1, 0);
    check(tp1, tp2, -1);
    check(tp3, tp4, -1);
    check(tp2, tp4, 1);
    check(tp3, tp5, -1);
    check(tp5, tp6, 1);
    check(tp5, tp7, -1);
  }

  @Test
  public void testFirstComparator() throws IOException {
    RawComparator comp = new TextPair.FirstComparator();
    check(comp, tp1, tp1, 0);
    check(comp, tp1, tp2, -1);
    check(comp, tp3, tp4, -1);
    check(comp, tp2, tp4, 1);
    check(comp, tp3, tp5, -1);
    check(comp, tp5, tp6, 0);
    check(comp, tp5, tp7, 0);
  }
  
  private void check(TextPair tp1, TextPair tp2, int c) throws IOException {
    check(WritableComparator.get(TextPair.class), tp1, tp2, c);
  }
  
  private void check(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    checkOnce(comp, tp1, tp2, c);
    checkOnce(comp, tp2, tp1, -c);
  }

  private void checkOnce(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    assertThat("Object", signum(comp.compare(tp1, tp2)), is(c));
    byte[] out1 = serialize(tp1);
    byte[] out2 = serialize(tp2);
    assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
  }
  
  private int signum(int i) {
    return i < 0 ? -1 : (i == 0 ? 0 : 1);
  }

}
