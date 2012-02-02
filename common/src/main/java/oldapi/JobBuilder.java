package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class JobBuilder {
  
  private final Class<?> driverClass;
  private final JobConf conf;
  private final int extraArgCount;
  private final String extrArgsUsage;
  
  private String[] extraArgs;
  
  public JobBuilder(Class<?> driverClass) {
    this(driverClass, 0, "");
  }
  
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    this.conf = new JobConf(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  public static JobConf parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) {
    
    if (args.length != 2) {
      printUsage(tool, "<input> <output>");
      return null;
    }
    JobConf jobConf = new JobConf(conf, tool.getClass());
    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    return jobConf;
  }

  public static void printUsage(Tool tool, String extraArgsUsage) {
    System.err.printf("Usage: %s [genericOptions] %s\n\n",
        tool.getClass().getSimpleName(), extraArgsUsage);
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public JobBuilder withCommandLineArgs(String... args) throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
      System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
          driverClass.getSimpleName(), extrArgsUsage);
      GenericOptionsParser.printGenericCommandUsage(System.err);
      System.exit(-1);
    }
    int index = 0;
    boolean overwrite = false;
    if (otherArgs[index].equals("-overwrite")) {
      overwrite = true;
      index++;
    }
    Path input = new Path(otherArgs[index++]);
    Path output = new Path(otherArgs[index++]);
    
    if (index < otherArgs.length) {
      extraArgs = new String[otherArgs.length - index];
      System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
    }
    
    if (overwrite) {
      output.getFileSystem(conf).delete(output, true);
    }
    
    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    return this;
  }
  
  public JobConf build() {
    return conf;
  }
  
  public String[] getExtraArgs() {
    return extraArgs;
  }
}
