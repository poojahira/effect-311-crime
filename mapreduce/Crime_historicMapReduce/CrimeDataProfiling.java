import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CrimeDataProfiling {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(CrimeDataProfiling.class);
		job.setJobName("Crime Data Profiling");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CrimeMapper.class);
		job.setReducerClass(CrimeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
