package crunch;

import java.util.Iterator;
import org.apache.crunch.MapFn;

public class CountValuesFn<S> extends MapFn<Iterable<S>, Integer> {
  @Override
  public Integer map(Iterable<S> input) {
    int count = 0;
    for (Iterator i = input.iterator(); i.hasNext(); ) {
      i.next();
      count++;
    }
    return count;
  }
}
