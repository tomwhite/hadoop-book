package crunch;

import java.util.regex.Pattern;
import org.apache.crunch.FilterFn;

public class PatternFilterFn extends FilterFn<String> {

  transient Pattern pattern;

  @Override
  public void initialize() {
    pattern = Pattern.compile("[A-Za-z0-9]");
  }

  @Override
  public boolean accept(String input) {
    return pattern.matcher(input).matches();
  }
}
