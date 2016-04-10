package crunch;

import java.io.IOException;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Sort;
import org.junit.Before;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;

public class SortTest {

  private PTable<Integer, Integer> a;

  @Before
  public void setup() {
    a = MemPipeline.typedTableOf(tableOf(ints(), ints()),
        2, 3, 1, 2, 2, 4);
  }

  @Test
  public void testSortTableByKey() throws IOException {
    assertEquals("{(2,3),(1,2),(2,4)}", dump(a));
    PTable<Integer, Integer> b = Sort.sort(a);
    assertEquals("{(1,2),(2,3),(2,4)}", dump(b));
  }

  // use sortPairs to treat as pairs, so we can impose an order on the value

  // how to sort an Avro object? need to use specific to generate it with order fields
  // as discussed in the Avro chapter.

}
