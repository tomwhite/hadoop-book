import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxWidgetIdGenericAvro extends Configured implements Tool {

  public static class MaxWidgetMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,
      AvroKey<Long>, AvroValue<GenericRecord>> {

    private GenericRecord maxWidget;
    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      GenericRecord widget = key.datum();
      Integer id = (Integer) widget.get("id");
      if (id != null) {
        if (maxWidget == null
            || id > (Integer) maxWidget.get("id")) {
          maxWidget = widget;
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new AvroKey(0L), new AvroValue<GenericRecord>(maxWidget));
    }
  }

  public static class MaxWidgetReducer
      extends Reducer<AvroKey<Long>, AvroValue<GenericRecord>,
      AvroKey<GenericRecord>, NullWritable> {

    @Override
    protected void reduce(AvroKey<Long> key, Iterable<AvroValue<GenericRecord>>
        values, Context context) throws IOException, InterruptedException {
      GenericRecord maxWidget = null;
      for (AvroValue<GenericRecord> value : values) {
        GenericRecord record = value.datum();
        if (maxWidget == null
            || (Integer) record.get("id") > (Integer) maxWidget.get("id")) {
          maxWidget = copy(record);
        }
      }
      context.write(new AvroKey(maxWidget), NullWritable.get());
    }

    private GenericRecord copy(GenericRecord record) {
      Schema schema = record.getSchema();
      GenericRecord copy = new GenericData.Record(schema);
      for (Schema.Field f : schema.getFields()) {
        copy.put(f.name(), record.get(f.name()));
      }
      return copy;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), "Max widget ID");
    job.setJarByClass(getClass());

    job.getConfiguration().setBoolean(
        Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    Path inputDir = new Path("widgets");
    FileInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    Schema schema = readSchema(inputDir, getConf());

    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
    AvroJob.setMapOutputValueSchema(job, schema);
    AvroJob.setOutputKeySchema(job, schema);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    job.setMapperClass(MaxWidgetMapper.class);
    job.setReducerClass(MaxWidgetReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * Read the Avro schema from the first file in the input directory.
   */
  private Schema readSchema(Path inputDir, Configuration conf) throws IOException {
    FsInput fsInput = null;
    FileReader<Object> reader = null;
    try {
      fsInput = new FsInput(new Path(inputDir, "part-m-00000.avro"), conf);
      reader = DataFileReader.openReader(fsInput, new GenericDatumReader<Object>());
      return reader.getSchema();
    } finally {
      IOUtils.closeStream(fsInput);
      IOUtils.closeStream(reader);
    }
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetIdGenericAvro(), args);
    System.exit(ret);
  }
}
