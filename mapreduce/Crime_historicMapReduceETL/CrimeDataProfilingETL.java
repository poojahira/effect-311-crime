import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CrimeDataProfilingETL {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(CrimeDataProfilingETL.class);
		job.setJobName("Crime Data Profiling");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CrimeMapperETL.class);
		job.setReducerClass(CrimeReducerETL.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
