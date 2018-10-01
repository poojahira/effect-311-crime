import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CenterlineMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String discard = "";
		if (key.get() == 0) 
			discard = value.toString();
		else {	
			String line = value.toString();
			String[] data = line.split("\\|",-1);
			context.write(new Text(data[0]), new Text(data[2]+ "|" + data[10] + "|" + data[13] + "|" + data[31]));
		}
	}
}
