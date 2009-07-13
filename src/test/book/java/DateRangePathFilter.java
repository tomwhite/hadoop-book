import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DateRangePathFilter implements PathFilter {
  
  private final Pattern PATTERN = Pattern.compile("^.*/(\\d\\d\\d\\d/\\d\\d/\\d\\d).*$");
  
  private final Date start, end;

  public DateRangePathFilter(Date start, Date end) {
    this.start = new Date(start.getTime());
    this.end = new Date(end.getTime());
  }
  
  public boolean accept(Path path) {
    Matcher matcher = PATTERN.matcher(path.toString());
    if (matcher.matches()) {
      DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
      try {
        return inInterval(format.parse(matcher.group(1)));
      } catch (ParseException e) {
        return false;
      }
    }
    return false;
  }

  private boolean inInterval(Date date) {
    return !date.before(start) && !date.after(end);
  }

}
