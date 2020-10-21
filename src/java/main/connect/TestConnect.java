package connect;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class TestConnect {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.0.101:9000");
		FileSystem fs = FileSystem.get(uri, conf, "hadoop");
		System.out.println(fs);
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath());
		}
		fs.close();
	}
}
