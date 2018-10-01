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
				Date complaint_date = formatter.parse(data[1]);
			if (data.length >= 23 && data[1] != null && !data[1].isEmpty() && data[19] != null && !data[19].isEmpty() && data[20] != null && !data[20].isEmpty() && complaint_date.after(formatter.parse("12/31/2009")) && complaint_date.before(formatter.parse("04/01/2018"))){
				data[7] = data[7].toLowerCase();
				data[11] = data[11].toLowerCase();
				data[13] = data[13].toLowerCase();
				data[16]= data[16].toLowerCase();
				context.write(new Text(data[0]), new Text(data[1]+ "|" + data[6] + "|" + data[7] + "|" + data[11]+ "|" + data[16] + "|" + data[13] + "|" + data[19] + "|" + data[20] + "|" + data[21] + "|" + data[22]));
			}}
			catch(ParseException exception) {exception.printStackTrace();}
		}

	}
}
