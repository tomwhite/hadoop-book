import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

public class FileDecompressorTest {

  @Test
  public void decompressesGzippedFile() throws Exception {
    File file = new File("src/test/book/data/file");
    FileUtil.fullyDelete(file);
    
    FileDecompressor.main(new String[] { "src/test/book/data/file.gz" });
    
    assertThat(readFile(file), is("Text\n"));
  }
  
  private String readFile(File file) throws IOException {
    return new Scanner(file).useDelimiter("\\A").next();
  }
                                  
}
