package v1;
// == MaxTemperatureReducerTestV1

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.*;

public class MaxTemperatureReducerTest {
  
  //vv MaxTemperatureReducerTestV1
  @Test
  public void returnsMaximumIntegerInValues() throws IOException {
    MaxTemperatureReducer reducer = new MaxTemperatureReducer();
    
    Text key = new Text("1950");
    Iterator<IntWritable> values = Arrays.asList(
        new IntWritable(10), new IntWritable(5)).iterator();
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
    
    reducer.reduce(key, values, output, null);
    
    verify(output).collect(key, new IntWritable(10));
  }
  //^^ MaxTemperatureReducerTestV1
}
