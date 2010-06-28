package v5;
// == MaxTemperatureMapperTestV5Malformed
import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void parsesValidRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
    		"99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    mapper.map(null, value, output, null);
    
    verify(output).collect(new Text("1950"), new IntWritable(-11));
  }
  
  @Test
  public void parsesMissingTemperature() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    mapper.map(null, value, output, null);
    
    verify(output, never()).collect(any(Text.class), any(IntWritable.class));
  }
//vv MaxTemperatureMapperTestV5Malformed
  @Test
  public void parsesMalformedTemperature() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
    Reporter reporter = mock(Reporter.class);

    mapper.map(null, value, output, reporter);
    
    verify(output, never()).collect(any(Text.class), any(IntWritable.class));
    verify(reporter).incrCounter(MaxTemperatureMapper.Temperature.MALFORMED, 1);
  }
// ^^ MaxTemperatureMapperTestV5Malformed
}
