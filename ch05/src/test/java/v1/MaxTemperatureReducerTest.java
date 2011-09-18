package v1;
// == MaxTemperatureReducerTestV1

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.junit.*;

public class MaxTemperatureReducerTest {
  
  //vv MaxTemperatureReducerTestV1
  @Test
  public void returnsMaximumIntegerInValues() throws IOException,
      InterruptedException {
    MaxTemperatureReducer reducer = new MaxTemperatureReducer();
    
    Text key = new Text("1950");
    List<IntWritable> values = Arrays.asList(
        new IntWritable(10), new IntWritable(5));
    MaxTemperatureReducer.Context context =
      mock(MaxTemperatureReducer.Context.class);
    
    reducer.reduce(key, values, context);
    
    verify(context).write(key, new IntWritable(10));
  }
  //^^ MaxTemperatureReducerTestV1
}
