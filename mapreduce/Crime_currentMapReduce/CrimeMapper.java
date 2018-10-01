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
			if (data.length >= 32 && data[3] != null && !data[3].isEmpty() && data[30] != null && !data[30].isEmpty() && data[31] != null && !data[31].isEmpty()){
				if (data[0] != null && !data[0].isEmpty())
	                                context.write(new Text("Complaint_Number"),new Text(data[0]));
				if (data[2] != null && !data[2].isEmpty())
	                                context.write(new Text("Borough"),new Text(data[2]));
				if (data[12] != null && !data[12].isEmpty())
	                                context.write(new Text("Offence_Code"),new Text(data[12]));
				if (data[13] != null && !data[13].isEmpty())
	                                context.write(new Text("Offence_Category"),new Text(data[13]));
				if (data[15] != null && !data[15].isEmpty())
	                                context.write(new Text("Offence_Description"),new Text(data[15]));
				if (data[20] != null && !data[20].isEmpty()) 
	                                context.write(new Text("Premises_Type"),new Text(data[20]));
	            		context.write(new Text("Date_Crime_Committed"),new Text(data[3]));
				context.write(new Text("X-Coordinate"),new Text(data[30]));
	            		context.write(new Text("Y-Coordinate"),new Text(data[31]));
				
				if (data.length >= 33) {
					if (data[32] != null && !data[32].isEmpty())
						context.write(new Text("Latitude"),new Text(data[32]));
					if (data[33] != null && !data[33].isEmpty()) 
						context.write(new Text("Longitude"),new Text(data[33]));
				}
			}
		}

	}
}
