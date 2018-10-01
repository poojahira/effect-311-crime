import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class Reducer311ETL extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	}
}