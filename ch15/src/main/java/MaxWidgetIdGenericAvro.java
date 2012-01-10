import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxWidgetIdGenericAvro extends Configured implements Tool {
  
  public static class MaxWidgetMapper extends AvroMapper<GenericRecord, Pair<Long, GenericRecord>> {
    
    private GenericRecord maxWidget;
    private AvroCollector<Pair<Long, GenericRecord>> collector;
    
    @Override
    public void map(GenericRecord widget,
        AvroCollector<Pair<Long, GenericRecord>> collector, Reporter reporter)
        throws IOException {
      this.collector = collector;
      Integer id = (Integer) widget.get("id");
      if (id != null) {
        if (maxWidget == null
            || id > (Integer) maxWidget.get("id")) {
          maxWidget = widget;
        }
      }
    }
    
    @Override
    public void close() throws IOException {
      if (maxWidget != null) {
        collector.collect(new Pair<Long, GenericRecord>(0L, maxWidget));
      }
      super.close();
    }
    
  }
  
  static GenericRecord copy(GenericRecord record) {
    Schema schema = record.getSchema();
    GenericRecord copy = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      copy.put(f.name(), record.get(f.name()));
    }
    return copy;
  }
  
  public static class MaxWidgetReducer extends AvroReducer<Long, GenericRecord, GenericRecord> {
    
    @Override
    public void reduce(Long key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
      GenericRecord maxWidget = null;

      for (GenericRecord w : values) {
        if (maxWidget == null
            || (Integer) w.get("id") > (Integer) maxWidget.get("id")) {
          maxWidget = copy(w);
        }
      }

      if (maxWidget != null) {
        collector.collect(maxWidget);
      }
    }
  }

  public int run(String [] args) throws Exception {
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    Path inputDir = new Path("widgets");
    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, new Path("maxwidget"));
    
    Schema schema = readSchema(inputDir, conf);
    
    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
        
    AvroJob.setInputSchema(conf, schema);
    AvroJob.setMapOutputSchema(conf,
        Pair.getPairSchema(Schema.create(Schema.Type.LONG), schema));
    AvroJob.setOutputSchema(conf, schema);

    AvroJob.setMapperClass(conf, MaxWidgetMapper.class);
    AvroJob.setReducerClass(conf, MaxWidgetReducer.class);

    conf.setNumReduceTasks(1);

    JobClient.runJob(conf);
    return 0;
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
