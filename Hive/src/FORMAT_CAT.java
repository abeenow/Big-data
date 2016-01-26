import org.apache.hadoop.hive.ql.exec.UDF;
 
public final class FORMAT_CAT extends UDF {
  public String evaluate(final String s) {
    if (s == null) { return null; }
    return (s.replaceAll(",", "|"));
  }
}
