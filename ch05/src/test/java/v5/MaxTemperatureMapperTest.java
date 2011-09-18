package v5;
// == MaxTemperatureMapperTestV5Malformed
import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void parsesValidRecord() throws IOException, InterruptedException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    MaxTemperatureMapper.Context context =
      mock(MaxTemperatureMapper.Context.class);
    
    mapper.map(null, value, context);
    
    verify(context).write(new Text("1950"), new IntWritable(-11));
  }
  
  @Test
  public void parsesMissingTemperature() throws IOException,
      InterruptedException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    MaxTemperatureMapper.Context context =
      mock(MaxTemperatureMapper.Context.class);
    
    mapper.map(null, value, context);
    
    verify(context, never()).write(any(Text.class), any(IntWritable.class));
  }
//vv MaxTemperatureMapperTestV5Malformed
  @Test
  public void parsesMalformedTemperature() throws IOException,
      InterruptedException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    MaxTemperatureMapper.Context context =
      mock(MaxTemperatureMapper.Context.class);
    Counter counter = mock(Counter.class);
    when(context.getCounter(MaxTemperatureMapper.Temperature.MALFORMED))
      .thenReturn(counter);
    
    mapper.map(null, value, context);
    
    verify(context, never()).write(any(Text.class), any(IntWritable.class));
    verify(counter).increment(1);
  }
// ^^ MaxTemperatureMapperTestV5Malformed
}
