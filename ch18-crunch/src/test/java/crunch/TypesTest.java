package crunch;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Tuple3;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TypesTest implements Serializable {

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testAvroReflect() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("sample.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    PCollection<WeatherRecord> records = lines.parallelDo(
        new DoFn<String, WeatherRecord>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<WeatherRecord> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(new WeatherRecord(parser.getYearInt(),
              parser.getAirTemperature(), parser.getStationId()));
        }
      }
    }, Avros.records(WeatherRecord.class));

    PTable<Pair<Integer, Integer>, WeatherRecord> table = records.by(
        new MapFn<WeatherRecord, Pair<Integer, Integer>>() {
      @Override
      public Pair<Integer, Integer> map(WeatherRecord input) {
        return Pair.of(input.getYear(), input.getTemperature());
      }
    }, Avros.pairs(Avros.ints(), Avros.ints()));

    PCollection<WeatherRecord> sortedRecords = Sort.sort(table).values();

    Iterable<WeatherRecord> materialized = sortedRecords.materialize();

    List<WeatherRecord> expectedContent = Lists.newArrayList(
        new WeatherRecord(1949, 78, "012650-99999"),
        new WeatherRecord(1949, 111, "012650-99999"),
        new WeatherRecord(1950, -11, "011990-99999"),
        new WeatherRecord(1950, 0, "011990-99999"),
        new WeatherRecord(1950, 22, "011990-99999")
    );
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    pipeline.done();
  }

  @Test
  public void testAvroReflectSecondarySort() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("sample.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    PCollection<WeatherRecord> records = lines.parallelDo(
        new DoFn<String, WeatherRecord>() {
          NcdcRecordParser parser = new NcdcRecordParser();
          @Override
          public void process(String input, Emitter<WeatherRecord> emitter) {
            parser.parse(input);
            if (parser.isValidTemperature()) {
              emitter.emit(new WeatherRecord(parser.getYearInt(),
                  parser.getAirTemperature(), parser.getStationId()));
            }
          }
        }, Avros.records(WeatherRecord.class));

    PCollection<Tuple3<Integer, Integer, WeatherRecord>> triples = records.parallelDo(
        new DoFn<WeatherRecord, Tuple3<Integer, Integer, WeatherRecord>>() {
          @Override
          public void process(WeatherRecord input,
              Emitter<Tuple3<Integer, Integer, WeatherRecord>> emitter) {
            emitter.emit(Tuple3.of(input.getYear(), input.getTemperature(), input));
          }
        }, Avros.triples(Avros.ints(), Avros.ints(), Avros.records(WeatherRecord.class))
    );

    PCollection<Tuple3<Integer, Integer, WeatherRecord>> sorted = Sort
        .sortTriples(triples, Sort.ColumnOrder.by(1, Sort.Order.ASCENDING),
            Sort.ColumnOrder.by(2, Sort.Order.DESCENDING));
    PCollection<WeatherRecord> sortedRecords = sorted.parallelDo(new DoFn<Tuple3<Integer,
        Integer, WeatherRecord>, WeatherRecord>() {
      @Override
      public void process(Tuple3<Integer, Integer, WeatherRecord> input,
          Emitter<WeatherRecord> emitter) {
        emitter.emit(input.third());
      }
    }, Avros.records(WeatherRecord.class));

    Iterable<WeatherRecord> materialized = sortedRecords.materialize();

    List<WeatherRecord> expectedContent = Lists.newArrayList(
        new WeatherRecord(1949, 111, "012650-99999"),
        new WeatherRecord(1949, 78, "012650-99999"),
        new WeatherRecord(1950, 22, "011990-99999"),
        new WeatherRecord(1950, 0, "011990-99999"),
        new WeatherRecord(1950, -11, "011990-99999")
    );
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    pipeline.done();
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testWriteWritableToAvroFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    a.write(To.avroFile(outputPath)); // fails with cannot serialize PType of class: class org.apache.crunch.types.writable.WritableType
    pipeline.done();
  }

  @Test
  public void testWriteAvroToSequenceFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> a = pipeline.read(From.textFile(inputPath, Avros.strings()));
    a.write(To.sequenceFile(outputPath)); // fails with Could not find a serializer for the Key class: 'org.apache.avro.mapred.AvroKey'.
    PipelineResult result = pipeline.done();
    assertFalse(result.succeeded());
  }

  @Test
  public void testConvertWritableToAvro() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    PCollection<String> b = a.parallelDo(IdentityFn.<String>getInstance(), Avros.strings());
    b.write(To.avroFile(outputPath));
    pipeline.done();
  }

  @Test
  public void testWriteTextToSequenceFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    assertEquals(WritableTypeFamily.getInstance(), lines.getPType().getFamily());

    String outputPath = tmpDir.getFileName("out");
    lines.write(To.sequenceFile(outputPath));
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(getClass());
    PTable<NullWritable, Text> d = pipeline2.read(From.sequenceFile(outputPath,
        NullWritable.class, Text.class));
    PCollection<Text> e = d.values();
    assertEquals("{2,3,1,3}", dump(e));

    pipeline2.done();
  }

}
