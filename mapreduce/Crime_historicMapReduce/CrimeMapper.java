import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String discard = "";
		if (key.get() == 0) 
			discard = value.toString();
		else {	
			String line = value.toString();
			String[] data = line.split("\t",-1);
			if (data.length >= 21 && data[1] != null && !data[1].isEmpty() && data[19] != null && !data[19].isEmpty() && data[20] != null && !data[20].isEmpty()){
				if (data[0] != null && !data[0].isEmpty())
	                                context.write(new Text("Complaint_Number"),new Text(data[0]));
				if (data[6] != null && !data[6].isEmpty())
	                                context.write(new Text("Offence_Code"),new Text(data[6]));
				if (data[7] != null && !data[7].isEmpty())
	                                context.write(new Text("Offence_Description"),new Text(data[7]));
				if (data[11] != null && !data[11].isEmpty())
	                                context.write(new Text("Offence_Category"),new Text(data[11]));
				if (data[13] != null && !data[13].isEmpty())
	                                context.write(new Text("Borough"),new Text(data[13]));
				if (data[16] != null && !data[16].isEmpty()) 
	                                context.write(new Text("Premises_Type"),new Text(data[16]));
	            		context.write(new Text("Date_Crime_Committed"),new Text(data[1]));
				context.write(new Text("X-Coordinate"),new Text(data[19]));
	            		context.write(new Text("Y-Coordinate"),new Text(data[20]));
				
				if (data.length >= 23) {
					if (data[21] != null && !data[21].isEmpty())
						context.write(new Text("Latitude"),new Text(data[21]));
					if (data[22] != null && !data[22].isEmpty()) 
						context.write(new Text("Longitude"),new Text(data[22]));
				}
			}
		}

	}
}
