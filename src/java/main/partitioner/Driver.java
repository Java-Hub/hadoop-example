package partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(MyPartitioner.class);

		job.setNumReduceTasks(17);

		FileInputFormat.addInputPath(job, new Path("/test"));

		FileOutputFormat.setOutputPath(job, new Path("/out/partitioner"));

		job.waitForCompletion(true);

	}

}
