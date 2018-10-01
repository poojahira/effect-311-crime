import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Mapper311ETL extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String discard = "";
		if (key.get() == 0) 
			discard = value.toString();
		else {	
			String line = value.toString();
			String[] data = line.split("\t",-1);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
			try {
				Date date_created = formatter.parse(data[1]);
			if (data.length >= 40 && data[1] != null && !data[1].isEmpty() && data[5] != null && !data[5].isEmpty() && data[26] != null && !data[26].isEmpty() && Integer.parseInt(data[26]) > 0 && data[27] != null && !data[27].isEmpty() && Integer.parseInt(data[27]) > 0 && date_created.before(formatter.parse("03/31/2018 23:59:59 PM"))){
				try {
				Date date_closed = formatter.parse(data[2]);
				Date date_lower_limit = formatter.parse("01/01/2010 01:00:00 AM");
				Date date_upper_limit = formatter.parse("07/12/2018 12:59:28 PM");
				if (date_closed.compareTo(date_lower_limit) < 0 || date_closed.compareTo(date_upper_limit) > 0) {
					data[2] = "";
				} }
				catch(ParseException exception) {exception.printStackTrace();}
				data[5] = data[5].toLowerCase();
				data[6] = data[6].toLowerCase();
				data[21] = data[21].toLowerCase();
				data[25]= data[25].toLowerCase();
				context.write(new Text(data[0]), new Text(data[1]+ "|" + data[2] + "|" + data[3] + "|" + data[5]+ "|" + data[6] + "|" + data[19] + "|" + data[21] + "|" + data[24] + "|" + data[25] + "|" + data[26]+ "|" + data[27] + "|" + data[38] + "|" + data[39]));
			}}
			catch(ParseException exception) {exception.printStackTrace();}
			catch(NumberFormatException exception) {exception.printStackTrace();}
		}
	}
}
