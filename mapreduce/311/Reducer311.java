import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class Reducer311 extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (key.toString().equals("Unique_Key") || key.toString().equals("BBL") || key.toString().equals("X-Coordinate") || key.toString().equals("Y-Coordinate")){
			long maxValue = Long.MIN_VALUE;
			long minValue = Long.MAX_VALUE;
			for (Text value : values) {
				try{
				long val = Long.parseLong(value.toString());
				maxValue = Math.max(maxValue,val);
				minValue = Math.min(minValue,val);}
				catch(NumberFormatException exception) {exception.printStackTrace();}
			}
			context.write(key,new Text("Range of data: " + Long.toString(minValue) + " - " + Long.toString(maxValue)));
		}

		else if (key.toString().equals("Date_Created") || key.toString().equals("Date_Closed")) {
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
			Date maxDate = new Date(0);
			Date minDate = new Date();
			for (Text value : values) {
				try {
					Date val = formatter.parse(value.toString());
					if (maxDate.before(val))
                                        	maxDate = val;
                                	if (minDate.after(val))
                                        	minDate = val;
					
				}
				catch(ParseException exception) {exception.printStackTrace();}
			}
			context.write(key,new Text("Range of dates: " + formatter.format(minDate) + " - " + formatter.format(maxDate)));
		}
		else if (key.toString().equals("Agency") || key.toString().equals("Borough") || key.toString().equals("Complaint_Status") || key.toString().equals("Complaint_Type")) {
			Set<String> valset = new HashSet<String>();
			for (Text value : values) {
				valset.add(value.toString());
			}
			String joined = String.join(",",valset);
			context.write(key,new Text("List of possible values: " +joined));
		}
		else if (key.toString().equals("Complaint_Descriptor") || key.toString().equals("Resolution_Description")) {
			int maxLength = Integer.MIN_VALUE;
			for (Text value : values) {
				maxLength = Math.max(maxLength,value.toString().length());
			}
			context.write(key,new Text("Max length of data: " + Integer.toString(maxLength)));
		}
		else if (key.toString().equals("Latitude") || key.toString().equals("Longitude")){
                        double maxValue = -Double.MAX_VALUE;
                        double minValue = Double.MAX_VALUE;
                        for (Text value : values) {
				try {
                                double val = Double.parseDouble(value.toString());
                                maxValue = Math.max(maxValue,val);
                                minValue = Math.min(minValue,val);}
				catch(NumberFormatException exception) {exception.printStackTrace();}
                        }
			context.write(key,new Text("Range of data: " + Double.toString(minValue) + " - " + Double.toString(maxValue)));
                }
	}
}
