//cc ListGroup A program to list the members in a group
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv ListGroup
public class ListGroup extends ConnectionWatcher {
    
  public void list(String groupName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName;
    
    try {
      List<String> children = zk.getChildren(path, false);
      if (children.isEmpty()) {
        System.out.printf("No members in group %s\n", groupName);
        System.exit(1);
      }
      for (String child : children) {
        System.out.println(child);
      }
    } catch (KeeperException.NoNodeException e) {
      System.out.printf("Group %s does not exist\n", groupName);
      System.exit(1);
    }
  }
  
  public static void main(String[] args) throws Exception {
    ListGroup listGroup = new ListGroup();
    listGroup.connect(args[0]);
    listGroup.list(args[1]);
    listGroup.close();
  }
}
// ^^ ListGroup
