package crunch;

import java.io.IOException;
import java.util.Collection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Cogroup;
import org.apache.crunch.lib.Join;
import org.junit.Before;
import org.junit.Test;

import static crunch.PCollections.dump;
import static crunch.PCollections.invert;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;

public class JoinTest {

  private PTable<Integer, String> a;
  private PTable<Integer, String> b;

  @Before
  public void setup() {
    a = MemPipeline.typedTableOf(tableOf(ints(), strings()),
        2, "Tie", 4, "Coat", 3, "Hat", 1, "Scarf");
    PTable<String, Integer> b0 = MemPipeline.typedTableOf(tableOf(strings(), ints()),
        "Joe", 2, "Hank", 4, "Ali", 0, "Eve", 3, "Hank", 2);
    b = invert(b0);
  }

  @Test
  public void testInner() throws IOException {
    assertEquals("{(2,Tie),(4,Coat),(3,Hat),(1,Scarf)}", dump(a));
    assertEquals("{(2,Joe),(4,Hank),(0,Ali),(3,Eve),(2,Hank)}", dump(b));
    PTable<Integer, Pair<String, String>> c = Join.join(a, b);
    assertEquals("{(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  @Test
  public void testLeft() throws IOException {
    PTable<Integer, Pair<String, String>> c = Join.leftJoin(a, b);
    assertEquals("{(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  @Test
  public void testRight() throws IOException {
    PTable<Integer, Pair<String, String>> c = Join.rightJoin(a, b);
    assertEquals("{(0,[null,Ali]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  @Test
  public void testFull() throws IOException {
    PTable<Integer, Pair<String, String>> c = Join.fullJoin(a, b);
    assertEquals("{(0,[null,Ali]),(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  @Test
  public void testCogroup() throws IOException {
    PTable<Integer, Pair<Collection<String>, Collection<String>>> c = Cogroup.cogroup(a, b);
    assertEquals("{(0,[[],[Ali]]),(1,[[Scarf],[]]),(2,[[Tie],[Joe, Hank]]),(3,[[Hat],[Eve]]),(4,[[Coat],[Hank]])}", dump(c));
  }
}
