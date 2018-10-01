import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CrimeMapperETL extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String discard = "";
		if (key.get() == 0) 
			discard = value.toString();
		else {	
			String line = value.toString();
			String[] data = line.split("\t",-1);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			try {
				Date complaint_date = formatter.parse(data[3]);
			if (data.length >= 34 && data[3] != null && !data[3].isEmpty() && data[30] != null && !data[30].isEmpty() && data[31] != null && !data[31].isEmpty() && complaint_date.after(formatter.parse("12/31/2009")) && complaint_date.before(formatter.parse("04/01/2018"))){
				data[2] = data[2].toLowerCase();
				data[13] = data[13].toLowerCase();
				data[15] = data[15].toLowerCase();
				data[20]= data[20].toLowerCase();
				context.write(new Text(data[0]), new Text(data[3]+ "|" + data[12] + "|" + data[15] + "|" + data[13]+ "|" + data[20] + "|" + data[2] + "|" + data[30] + "|" + data[31] + "|" + data[32] + "|" + data[33]));
			}}
			catch(ParseException exception) {exception.printStackTrace();}
		}

	}
}
