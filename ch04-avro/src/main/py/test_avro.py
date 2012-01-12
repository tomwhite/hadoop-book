import os
import unittest
from avro import schema
from avro import io
from avro import datafile

class TestAvro(unittest.TestCase):

  def test_container(self):
    writer = open('data.avro', 'wb')
    datum_writer = io.DatumWriter()
    schema_object = schema.parse("""\
{ "type": "record",
  "name": "StringPair",
  "doc": "A pair of strings.",
  "fields": [
    {"name": "left", "type": "string"},
    {"name": "right", "type": "string"}
  ]
}
    """)
    dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
    datum = {'left':'L', 'right':'R'}
    dfw.append(datum)
    dfw.close()
    
    reader = open('data.avro', 'rb')
    datum_reader = io.DatumReader()
    dfr = datafile.DataFileReader(reader, datum_reader)
    data = []
    for datum in dfr:
      data.append(datum)
      
    self.assertEquals(1, len(data));
    self.assertEquals(datum, data[0]);
    
  def test_write_data(self):
    writer = open('pairs.avro', 'wb')
    datum_writer = io.DatumWriter()
    schema_object = schema.parse(open('/Users/tom/workspace/hadoop-book-avro/src/main/java/Pair.avsc').read())
    dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
    dfw.append({'left':'a', 'right':'1'})
    dfw.append({'left':'c', 'right':'2'})
    dfw.append({'left':'b', 'right':'3'})
    dfw.append({'left':'b', 'right':'2'})
    dfw.close()
    
if __name__ == '__main__':
  unittest.main()
