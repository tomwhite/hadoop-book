package crunch;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;

public class ToYearTempPairsFn extends DoFn<String, Pair<String, Integer>> {

    private static final int MISSING = 9999;
	  

    @Override
    public void process(String input, Emitter<Pair<String, Integer>> emitter) {
      String line = input.toString();
      String year = line.substring(15, 19);
      int airTemperature;
      if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
        airTemperature = Integer.parseInt(line.substring(88, 92));
      } else {
        airTemperature = Integer.parseInt(line.substring(87, 92));
      }
      String quality = line.substring(92, 93);
      if (airTemperature != MISSING && quality.matches("[01459]")) {
        emitter.emit(Pair.of(year, airTemperature));
      }
    }

}
