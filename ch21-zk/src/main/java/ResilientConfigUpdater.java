//== ResilientConfigUpdater
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ResilientConfigUpdater {
  
  public static final String PATH = "/config";
  
  private ResilientActiveKeyValueStore store;
  private Random random = new Random();
  
  public ResilientConfigUpdater(String hosts) throws IOException,
      InterruptedException {
    store = new ResilientActiveKeyValueStore();
    store.connect(hosts);
  }
  
  public void run() throws InterruptedException, KeeperException {
    while (true) {
      String value = random.nextInt(100) + "";
      store.write(PATH, value);
      System.out.printf("Set %s to %s\n", PATH, value);
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }
  }
  
//vv ResilientConfigUpdater
  public static void main(String[] args) throws Exception {
    /*[*/while (true) {
      try {/*]*/
        ResilientConfigUpdater configUpdater =
          new ResilientConfigUpdater(args[0]);
        configUpdater.run();
      /*[*/} catch (KeeperException.SessionExpiredException e) {
        // start a new session
      } catch (KeeperException e) {
        // already retried, so exit
        e.printStackTrace();
        break;
      }
    }/*]*/
  }
//^^ ResilientConfigUpdater
}
