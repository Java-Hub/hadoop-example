package partitioner;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Text> {

	String[] partitions = new String[] { "上海交付部", "产品二部", "卫生交付部", "咨询一部", "咨询二部", "大数据产品部", "数据仓库部", "数据智能一部",
			"数据智能三部", "数据智能二部", "智能运维部", "珠海交付部", "生态技术服务部", "财税交付部", "财税咨询部", "金融交付部", "金融咨询部" };

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return Arrays.binarySearch(partitions, key.toString());
	}

}
