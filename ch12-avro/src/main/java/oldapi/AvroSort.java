package oldapi;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroSort extends Configured implements Tool {

  static class SortMapper<K> extends AvroMapper<K, Pair<K, K>> {
    public void map(K datum, AvroCollector<Pair<K, K>> collector,
        Reporter reporter) throws IOException {
      collector.collect(new Pair<K, K>(datum, null, datum, null));
    }
  }

  static class SortReducer<K> extends AvroReducer<K, K, K> {
    public void reduce(K key, Iterable<K> values,
        AvroCollector<K> collector,
        Reporter reporter) throws IOException {
      for (K value : values) {
        collector.collect(value);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 3) {
      System.err.printf(
        "Usage: %s [generic options] <input> <output> <schema-file>\n",
        getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    String input = args[0];
    String output = args[1];
    String schemaFile = args[2];

    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Avro sort");

    AvroJob.setDataModelClass(conf, GenericData.class);

    FileInputFormat.addInputPath(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));

    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    AvroJob.setInputSchema(conf, schema);
    Schema intermediateSchema = Pair.getPairSchema(schema, schema);
    AvroJob.setMapOutputSchema(conf, intermediateSchema);
    AvroJob.setOutputSchema(conf, schema);
    
    AvroJob.setMapperClass(conf, SortMapper.class);
    AvroJob.setReducerClass(conf, SortReducer.class);
  
    JobClient.runJob(conf); 
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroSort(), args);
    System.exit(exitCode);
  }
}
