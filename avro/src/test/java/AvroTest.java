import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class AvroTest {

  @Test
  public void testInt() throws IOException {
    Schema schema = Schema.parse("\"int\"");
    
    int datum = 163;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
    Encoder encoder = new BinaryEncoder(out);
    writer.write(datum, encoder); // boxed
    encoder.flush();
    out.close();
    
    DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema); // have to tell it the schema - it's not in the data stream!
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null /* reuse */);
    Integer result = reader.read(null /* reuse */, decoder);
    assertThat(result, is(163));
    
    try {
      reader.read(null, decoder);
      fail("Expected EOFException");
    } catch (EOFException e) {
      // expected
    }
  }
  
  @Test
  public void testPairGeneric() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = new BinaryEncoder(out);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
  }

  @Test
  public void testPairSpecific() throws IOException {
    
    Pair datum = new Pair();
    datum.left = new Utf8("L");
    datum.right = new Utf8("R");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Pair> writer = new SpecificDatumWriter<Pair>(Pair.class);
    Encoder encoder = new BinaryEncoder(out);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    DatumReader<Pair> reader = new SpecificDatumReader<Pair>(Pair.class);
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null);
    Pair result = reader.read(null, decoder);
    assertThat(result.left.toString(), is("L"));
    assertThat(result.right.toString(), is("R"));
  }

  @Test
  public void testDataFile() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
    
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(dataFileReader.hasNext(), is(false));
  }
  
  @Test
  public void testDataFileIteration() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    datum.put("right", new Utf8("r"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
    
  }
  
  @Test
  public void testSchemaResolution() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    Schema newSchema = Schema.parse(getClass().getResourceAsStream("NewPair.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = new BinaryEncoder(out);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description").toString(), is(""));
  }
  
  @Test
  public void testSchemaResolutionWithNull() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    Schema newSchema = Schema.parse(getClass().getResourceAsStream("NewPairWithNull.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = new BinaryEncoder(out);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description"), is((Object) null));
  }
  
  @Test
  public void testIncompatibleSchemaResolution() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    Schema newSchema = Schema.parse("{\"type\": \"array\", \"items\": \"string\"}");
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = new BinaryEncoder(out);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.defaultFactory()
      .createBinaryDecoder(out.toByteArray(), null);
    try {
      reader.read(null, decoder);
      fail("Expected AvroTypeException");
    } catch (AvroTypeException e) {
      // expected
    }

  }
  
  @Test
  public void testSchemaResolutionWithDataFile() throws IOException {
    Schema schema = Schema.parse(getClass().getResourceAsStream("Pair.avsc"));
    Schema newSchema = Schema.parse(getClass().getResourceAsStream("NewPair.avsc"));
    
    File file = new File("data.avro");
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("L"));
    datum.put("right", new Utf8("R"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(null, newSchema); // specify the read schema
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat(schema, is(dataFileReader.getSchema())); // schema is the actual (write) schema
    
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description").toString(), is(""));
    assertThat(dataFileReader.hasNext(), is(false));
  }
  
  // TODO: show specific types 

}
