import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class calls311 {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(calls311.class);
		job.setJobName("311 data ETL");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Mapper311.class);
		job.setReducerClass(Reducer311.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
