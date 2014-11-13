package oldapi;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {

    return new WholeFileRecordReader((FileSplit) split, job);
  }
}
