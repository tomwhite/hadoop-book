package crunch;

import java.io.IOException;
import java.io.Serializable;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.apache.crunch.types.avro.Avros.records;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TypesTest implements Serializable {

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  public static class MyRecord {
    private String value;
    public MyRecord() {
    }
    public MyRecord(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MyRecord myRecord = (MyRecord) o;
      if (value != null ? !value.equals(myRecord.value) : myRecord.value != null) {
        return false;
      }
      return true;
    }
    @Override
    public int hashCode() {
      return value != null ? value.hashCode() : 0;
    }
    @Override public String toString() {
      return "MyRecord(" + value + ")";
    }
  }

  @Test
  public void testAvroReflect() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String interPath = tmpDir.getFileName("inter");
    Pipeline pipeline = new MRPipeline(TypesTest.class);
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    PCollection<MyRecord> records = lines.parallelDo(new MapFn<String, MyRecord>() {
      @Override
      public MyRecord map(String input) {
        return new MyRecord(input);
      }
    }, Avros.records(MyRecord.class));
    PTable<MyRecord, Long> counts = records.count();
    assertEquals("{(MyRecord(1),1),(MyRecord(2),1),(MyRecord(3),2)}", dump(counts));

    records.write(To.avroFile(interPath));

    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(TypesTest.class);
    PCollection<MyRecord> records2 = pipeline2.read(From.avroFile(interPath,
        records(MyRecord.class)));
    assertEquals("{MyRecord(2),MyRecord(3),MyRecord(1),MyRecord(3)}", dump(records2));

    pipeline2.done();

  }

  @Test(expected = CrunchRuntimeException.class)
  public void testWriteWritableToAvroFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(TypesTest.class);
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    a.write(To.avroFile(outputPath)); // fails with cannot serialize PType of class: class org.apache.crunch.types.writable.WritableType
    pipeline.done();
  }

  @Test
  public void testWriteAvroToSequenceFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(TypesTest.class);
    PCollection<String> a = pipeline.read(From.textFile(inputPath, Avros.strings()));
    a.write(To.sequenceFile(outputPath)); // fails with Could not find a serializer for the Key class: 'org.apache.avro.mapred.AvroKey'.
    PipelineResult result = pipeline.done();
    assertFalse(result.succeeded());
  }

  @Test
  public void testConvertWritableToAvro() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(TypesTest.class);
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    PCollection<String> b = a.parallelDo(IdentityFn.<String>getInstance(), Avros.strings());
    b.write(To.avroFile(outputPath));
    pipeline.done();
  }

  @Test
  public void testWriteTextToSequenceFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = new MRPipeline(TypesTest.class);
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    assertEquals(WritableTypeFamily.getInstance(), lines.getPType().getFamily());

    String outputPath = tmpDir.getFileName("out");
    lines.write(To.sequenceFile(outputPath));
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(TypesTest.class);
    PTable<NullWritable, Text> d = pipeline2.read(From.sequenceFile(outputPath,
        NullWritable.class, Text.class));
    PCollection<Text> e = d.values();
    assertEquals("{2,3,1,3}", dump(e));

    pipeline2.done();
  }

}
