import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper311 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String discard = "";
		if (key.get() == 0) 
			discard = value.toString();
		else {	
			String line = value.toString();
			String[] data = line.split("\t",-1);
			if (data.length >= 28 && data[5] != null && !data[5].isEmpty() && data[26] != null && !data[26].isEmpty() && data[27] != null){
				if (data[0] != null && !data[0].isEmpty()) {
                                	context.write(new Text("Unique_Key"),new Text(data[0]));
                        	}
				if (data[1] != null && !data[1].isEmpty()) {
					context.write(new Text("Date_Created"),new Text(data[1]));
				}
				if (data[2] != null && !data[2].isEmpty()) {
                                	context.write(new Text("Date_Closed"),new Text(data[2]));
                        	}
				if (data[3] != null && !data[3].isEmpty()) {
                                	context.write(new Text("Agency"),new Text(data[3]));
                        	}
				if (data[6] != null && !data[6].isEmpty()) {
                                	context.write(new Text("Complaint_Descriptor"),new Text(data[6]));
                        	}
				if (data[19] != null && !data[19].isEmpty()) {
                                	context.write(new Text("Complaint_Status"),new Text(data[19]));
                        	}
				if (data[21] != null && !data[21].isEmpty()) {
                                	context.write(new Text("Resolution_Description"),new Text(data[21]));
                        	}
                        	if (data[24] != null && !data[24].isEmpty()) {
                                	context.write(new Text("BBL"),new Text(data[24]));
                        	}
				if (data[25] != null && !data[25].isEmpty()) {
                                	context.write(new Text("Borough"),new Text(data[25]));
                        	}
                        	context.write(new Text("Complaint_Type"),new Text(data[5]));
				context.write(new Text("X-Coordinate"),new Text(data[26]));
                        	context.write(new Text("Y-Coordinate"),new Text(data[27]));
				if (data.length >= 40) {
                        		if (data[38] != null && !data[38].isEmpty()) {
                                		context.write(new Text("Latitude"),new Text(data[38]));
                        		}
                        		if (data[39] != null && !data[39].isEmpty()) {
                                		context.write(new Text("Longitude"),new Text(data[39]));
                        		}
                		}
			}
		}
	}
}
