package crunch;
import static org.apache.crunch.lib.Sort.ColumnOrder.by;
import static org.apache.crunch.lib.Sort.Order.ASCENDING;
import static org.apache.crunch.lib.Sort.Order.DESCENDING;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.pairs;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.junit.Test;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import com.google.common.base.Splitter;

public class SortCrunchTest implements Serializable {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(SortCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("sort/A");
    
    PCollection<Pair<Integer, Integer>> pairs = records.parallelDo(new DoFn<String, Pair<Integer, Integer>>() {
      @Override
      public void process(String input, Emitter<Pair<Integer, Integer>> emitter) {
        Iterator<String> split = Splitter.on('\t').split(input).iterator();
        String l = split.next();
        String r = split.next();
        emitter.emit(Pair.of(Integer.parseInt(l), Integer.parseInt(r)));
      }
    }, pairs(ints(), ints()));
    
    PCollection<Pair<Integer, Integer>> sorted = Sort.sortPairs(pairs, by(1, ASCENDING), by(2, DESCENDING));
    
    pipeline.writeTextFile(sorted, "output-sorted");
    pipeline.run();
  }

}
