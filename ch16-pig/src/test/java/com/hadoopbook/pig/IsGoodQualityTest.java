package com.hadoopbook.pig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.pig.data.*;
import org.junit.*;

public class IsGoodQualityTest {
    
    private IsGoodQuality func;
    
    @Before
    public void setUp() {
      func = new IsGoodQuality();
    }

    @Test
    public void nullTuple() throws IOException {
      assertThat(func.exec(null), is(false));
    }
    
    @Test
    public void emptyTuple() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple();
      assertThat(func.exec(tuple), is(false));
    }

    @Test
    public void tupleWithNullField() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple((Object) null);
      assertThat(func.exec(tuple), is(false));
    }
    
    @Test
    public void badQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(2));
      assertThat(func.exec(tuple), is(false));
    }
    
    @Test
    public void goodQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(1));
      assertThat(func.exec(tuple), is(true));
    }
    

}
