package mapreduce.sorter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

public class TestMyKey {

	public static void main(String[] args) throws IOException {
		String str = "e:\\下载\\person.txt";
		List<String> allLines = Files.readAllLines(new File(str).toPath());
		allLines.forEach(s -> {
			String[] split = s.split("\t");
			Date date;
			try {
				date = MyKey.parse(split[3]);
			} catch (ParseException e) {
				date = new Date();
			}
			System.out.println(date);
		});

	}

}
