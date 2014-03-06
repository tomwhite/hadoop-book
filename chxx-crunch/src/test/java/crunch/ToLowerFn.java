package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class ToLowerFn extends DoFn<String, String> {
  @Override
  public void process(String input, Emitter<String> emitter) {
    emitter.emit(input.toLowerCase());
  }
}
