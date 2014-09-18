package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public class InversePairFn<S, T> extends DoFn<Pair<S, T>, Pair<T, S>> {
  @Override
  public void process(Pair<S, T> input, Emitter<Pair<T, S>> emitter) {
    emitter.emit(Pair.of(input.second(), input.first()));
  }
}
