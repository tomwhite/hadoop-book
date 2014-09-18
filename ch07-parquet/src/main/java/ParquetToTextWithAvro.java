import java.io.IOException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetInputFormat;

/**
 * Convert Parquet files to text using Parquet's {@code AvroParquetInputFormat}.
 */
public class ParquetToTextWithAvro extends Configured implements Tool {

  public static class ParquetToTextMapper
      extends Mapper<Void, IndexedRecord, NullWritable, Text> {

    @Override
    protected void map(Void key, IndexedRecord value, Context context)
        throws IOException, InterruptedException {
      context.write(NullWritable.get(), new Text(value.toString()));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job job = new Job(getConf(), "Parquet to text");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(ParquetToTextMapper.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(AvroParquetInputFormat.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ParquetToTextWithAvro(), args);
    System.exit(exitCode);
  }
}
