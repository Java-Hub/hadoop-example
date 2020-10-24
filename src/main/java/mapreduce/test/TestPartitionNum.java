package mapreduce.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


public class TestPartitionNum {

	public static void main(String[] args) throws IOException {
		String str = "E:\\下载\\info.txt";
		Path path = new File(str).toPath();
		List<String> lines = Files.readAllLines(path);
		Set<String> set = new TreeSet<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		for (String line : lines) {
			String[] split = line.split("\t");
			System.out.println(split[4]);
			set.add(split[4]);
		}
		System.out.println(set.size());
		System.out.println(set);
	}

}
