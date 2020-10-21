package sorter;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, MyKey, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String[] vals = val.split("\t");
		//// 2100001 北京 汤贺静 2007.4.5 女
		String id = vals[0];
		String base = vals[1];
		String name = vals[2];
		Date date;
		try {
			date = MyKey.parse(vals[3]);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		char sex = vals[4].charAt(0);
		MyKey mykey = new MyKey(id, base, name, date, sex);
		context.write(mykey, NullWritable.get());
	}

}
