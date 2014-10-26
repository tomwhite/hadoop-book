package com.hadoopbook.pig;

import org.apache.pig.PrimitiveEvalFunc;

//cc Trim An EvalFunc UDF to trim leading and trailing whitespace from chararray values
//vv Trim
public class Trim extends PrimitiveEvalFunc<String, String> {
  @Override
  public String exec(String input) {
    return input.trim();
  }
}
// ^^ Trim