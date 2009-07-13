import java.io.*;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair> {

  private int first;
  private int second;
  
  public IntPair() {
  }
  
  public IntPair(int first, int second) {
    set(first, second);
  }
  
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }
  
  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public int hashCode() {
    return first * 163 + second;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntPair) {
      IntPair ip = (IntPair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(IntPair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }
  
  /**
   * Convenience method for comparing two ints.
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}
