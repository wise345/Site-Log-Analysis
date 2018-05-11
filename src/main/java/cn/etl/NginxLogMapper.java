package cn.etl;

import java.io.IOException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NginxLogMapper  extends Mapper<LongWritable, Text, NullWritable, Text>
{
	private String regex = "([0-9\\.]+) - - \\[([^\\]]+)] \"([^\"]*)\" ([\\d]+) ([\\d]*) \"([^\"]*)\" \"([^\"]*)\" ";
	private Pattern pattern = Pattern.compile(regex);
	private StringBuilder buffer  = new StringBuilder(1024);
	private SimpleDateFormat nginxTimeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
	private SimpleDateFormat recordTimeFormat = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat TimeFormat = new SimpleDateFormat("HH:mm:ss");
	private Text result = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException 
	{
		Matcher matcher  = pattern.matcher(value.toString());
		if (!matcher.find())
		{
			context.getCounter("MY COUNTERS","erroe-lines").increment(1);
			return;
		}
		buffer.delete(0, buffer.length());
		//IP
		buffer.append(matcher.group(1)).append("\t");
		try {
			Date date  = nginxTimeFormat.parse(matcher.group(2));
			//TIMESTAMP
			buffer.append(date.getTime()).append("\t");
			//DATETIME
			buffer.append(recordTimeFormat.format(date)).append("\t");
			//TIME
			buffer.append(TimeFormat.format(date)).append("\t").append("\t");
		} catch (Exception e) {
			e.printStackTrace();
		}
		//URL
		buffer.append(matcher.group(3)).append("\t");
		//STATUS
		buffer.append(matcher.group(4)).append("\t");
		//SIZE
		buffer.append(matcher.group(5)).append("\t");
		//PAGE
		buffer.append(URLDecoder.decode(matcher.group(6),"utf-8")).append("\t");
		//INFORMATION
		buffer.append(matcher.group(7)).append("\t");
		//buffer.append(matcher.group(8)).append("\t");
		result.set(buffer.toString());
		context.write(NullWritable.get(), result);
	}
}
