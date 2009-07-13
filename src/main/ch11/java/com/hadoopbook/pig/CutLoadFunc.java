//cc CutLoadFunc A LoadFunc UDF to load tuple fields as column ranges
package com.hadoopbook.pig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.*;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

// vv CutLoadFunc
public class CutLoadFunc extends Utf8StorageConverter implements LoadFunc {

  private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte) '\n';

  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private BufferedPositionedInputStream in;
  private long end = Long.MAX_VALUE;
  private List<Range> ranges;

  public CutLoadFunc(String cutPattern) {
    ranges = Range.parse(cutPattern);
  }

  @Override
  public void bindTo(String fileName, BufferedPositionedInputStream in,
      long offset, long end) throws IOException {
    this.in = in;
    this.end = end;

    // Throw away the first (partial) record - it will be picked up by another
    // instance
    if (offset != 0) {
      getNext();
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    if (in == null || in.getPosition() > end) {
      return null;
    }

    String line;
    while ((line = in.readLine(UTF8, RECORD_DELIMITER)) != null) {
      Tuple tuple = tupleFactory.newTuple(ranges.size());
      for (int i = 0; i < ranges.size(); i++) {
        try {
          Range range = ranges.get(i);
          if (range.getEnd() > line.length()) {
            LOG.warn(String.format(
                "Range end (%s) is longer than line length (%s)",
                range.getEnd(), line.length()));
            continue;
          }
          tuple.set(i, new DataByteArray(range.getSubstring(line)));
        } catch (ExecException e) {
          throw new IOException(e);
        }
      }
      return tuple;
    }
    return null;
  }

  @Override
  public void fieldsToRead(Schema schema) {
    // Can't use this information to optimize, so ignore it
  }

  @Override
  public Schema determineSchema(String fileName, ExecType execType,
      DataStorage storage) throws IOException {
    // Cannot determine schema in general
    return null;
  }
}
// ^^ CutLoadFunc
