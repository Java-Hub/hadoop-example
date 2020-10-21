package sorter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey> {
	String id;
	String base;
	String name;
	Date date;
	char sex;

	public MyKey() {
		super();
	}

	public MyKey(String id, String base, String name, Date date, char sex) {
		super();
		this.id = id;
		this.base = base;
		this.name = name;
		this.date = date;
		this.sex = sex;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getBase() {
		return base;
	}

	public void setBase(String base) {
		this.base = base;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public char getSex() {
		return sex;
	}

	public void setSex(char sex) {
		this.sex = sex;
	}

	private static SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd");

	public static Date parse(String res) throws ParseException {
		try {
			return format.parse(res);
		} catch (ParseException e) {
			return new SimpleDateFormat("yyyy.MM").parse(res);
		}
	}

	public static String format(Date date) {
		return format.format(date);
	}

	public static int getSex(String res) {
		return "男".equals(res) ? 1 : "女".equals(res) ? 0 : -1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(base);
		out.writeUTF(name);
		out.writeUTF(format.format(date));
		out.writeUTF(sex+"");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.base = in.readUTF();
		this.name = in.readUTF();
		try {
			this.date = parse(in.readUTF());
		} catch (Exception e) {
			this.date = new Date();
		}
		this.sex = in.readUTF().charAt(0);
	}

	@Override
	public int compareTo(MyKey o) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String other = dateFormat.format(o.date);
		String thisdate = dateFormat.format(date);
		return Integer.parseInt(thisdate) - Integer.parseInt(other);
	}

	@Override
	public String toString() {
		return id + "," + base + "," + name + "," + format(date) + "," + sex;
	}

}
