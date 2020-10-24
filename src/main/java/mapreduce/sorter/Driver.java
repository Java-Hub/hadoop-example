package mapreduce.sorter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(MyKey.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("/person"));

		FileOutputFormat.setOutputPath(job, new Path("/out/sorter"));

		job.waitForCompletion(true);

	}

}
