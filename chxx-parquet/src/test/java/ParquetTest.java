import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.avro.AvroReadSupport;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ParquetTest {

  @Test
  public void testParquetFile() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message Pair {\n" +
        "  required binary left (UTF8);\n" +
        "  required binary right (UTF8);\n" +
        "}");

    GroupFactory groupFactory = new SimpleGroupFactory(schema);
    Group group = groupFactory.newGroup()
        .append("left", "L")
        .append("right", "R");

    Configuration conf = new Configuration();
    Path path = new Path("data.parquet");
    GroupWriteSupport writeSupport = new GroupWriteSupport();
    GroupWriteSupport.setSchema(schema, conf);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
        ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetProperties.WriterVersion.PARQUET_1_0, conf);
    writer.write(group);
    writer.close();

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader<Group> reader = new ParquetReader<Group>(path, readSupport);
    Group result = reader.read();
    assertNotNull(result);
    assertThat(result.getString("left", 0), is("L"));
    assertThat(result.getString("right", 0), is("R"));
    assertNull(reader.read());

    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(path, true);
  }

  @Test
  public void testParquetFileUsingAvro() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    Path path = new Path("data.parquet");
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    writer.write(datum);
    writer.close();

    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(path);
    GenericRecord result = reader.read();
    assertNotNull(result);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertNull(reader.read());

    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(path, true);
  }

  @Test
  public void testParquetFileUsingAvroProjection() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    Path path = new Path("data.parquet");
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    writer.write(datum);
    writer.close();

    parser = new Schema.Parser();
    Schema projectionSchema = parser.parse(
        getClass().getResourceAsStream("ProjectedStringPair.avsc"));
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projectionSchema);
    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(conf, path);
    GenericRecord result = reader.read();
    assertNull(result.get("left"));
    assertThat(result.get("right").toString(), is("R"));
    assertNull(reader.read());

    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(path, true);
  }

  @Test
  public void testParquetFileUsingAvroProjectionAndReadSchema() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    Path path = new Path("data.parquet");
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    writer.write(datum);
    writer.close();

    parser = new Schema.Parser();
    Schema projectionSchema = parser.parse(
        getClass().getResourceAsStream("ProjectedStringPair.avsc"));
    parser = new Schema.Parser();
    Schema readSchema = parser.parse(
        getClass().getResourceAsStream("NewStringPair.avsc"));
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projectionSchema);
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(conf, path);
    GenericRecord result = reader.read();
    assertNull(result.get("left"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description").toString(), is(""));
    assertNull(reader.read());

    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(path, true);
  }

}
