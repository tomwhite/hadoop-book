import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FileDecompressorTest {

  @Test
  public void decompressesGzippedFile() throws Exception {
    File file = File.createTempFile("file", ".gz");
    file.deleteOnExit();
    InputStream in = this.getClass().getResourceAsStream("/file.gz");
    IOUtils.copyBytes(in, new FileOutputStream(file), 4096, true);
    
    String path = file.getAbsolutePath();
    FileDecompressor.main(new String[] { path });
    
    String decompressedPath = path.substring(0, path.length() - 3);
    assertThat(readFile(new File(decompressedPath)), is("Text\n"));
  }
  
  private String readFile(File file) throws IOException {
    return new Scanner(file).useDelimiter("\\A").next();
  }
}
