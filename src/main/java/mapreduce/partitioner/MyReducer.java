package mapreduce.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		values.forEach(v -> sb.append("\t").append(v));
		sb.delete(0, 1);
		context.write(key, new Text(sb.toString()));
	}

}
