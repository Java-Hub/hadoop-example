package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String[] vals = val.split("\t");
		String k = vals[4];
		String v = vals[1] + "\t" + vals[2] + "\t" + vals[3];
		context.write(new Text(k), new Text(v));
	}

}
