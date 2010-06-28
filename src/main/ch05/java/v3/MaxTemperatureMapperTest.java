// == MaxTemperatureMapperTestV3
// vv MaxTemperatureMapperTestV3
package v3;

import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void processesValidRecord() throws IOException {
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
  public void processesPositiveTemperatureRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00111+99999999999");
                              // Temperature ^^^^^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    mapper.map(null, value, output, null);
    
    verify(output).collect(new Text("1950"), new IntWritable(11));
  }
  
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    mapper.map(null, value, output, null);
    
    verify(output, never()).collect(any(Text.class), any(IntWritable.class));
  }
  
  @Test
  public void ignoresSuspectQualityRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00112+99999999999");
                              // Temperature ^^^^^
                               // Suspect quality ^
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    mapper.map(null, value, output, null);
    
    verify(output, never()).collect(any(Text.class), any(IntWritable.class));
  }
  
}
//^^ MaxTemperatureMapperTestV3
