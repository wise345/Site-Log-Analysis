package cn.etl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogClean 
{
//	 create external table data
//	    > (remote_addr string,
//	    > time_stamp bigint,
//	    > time_local string,
//	    > time_date  string,
//	    > request    string,
//	    > status     string,
//	    > body_bytes_sent string,
//	    > http_referer string,
//	    > http_user_agent string)
//	    > partition by(year int,month int,day int)
//	    > row format delimited
//	    > fields terminated by '\t'
//	    > lines terminated by '\n'
//	    > stored as textfile;
	
//	create external table data
//	(	
//		remote_addr  string,
//		time_stamp   bigint,
//		time_local    string,
//		time_date    string,
//		request       string,
//		status        string,
//	 	body_bytes_sent string,
//		http_referer    string,
//		http_user_agent string
//	)partition by(year int,month int,day int)
//	row format delimited
//	fields terminated by '\t'
//	lines terminated by '\n'
//	stored as textfile;
	public static void main(String[] args) {
	//String s = "192.168.211.1 - - [10/Mar/2018:05:09:23 +0800] \"GET /data/view/index.html HTTP/1.1\" 200 12254 \"-\" \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\" \"-\"";
	
	String s = "192.168.211.1 - - [10/Mar/2018:05:09:23 +0800] \"GET /data/view/img/nav2.jpg HTTP/1.1\" 200 12633 \"http://s00/data/view/index.html\" \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\" \"-";
	
	String regex = "([0-9\\.]+) - - \\[([^\\]]+)] \"([^\"]*)\" ([\\d]+) ([\\d]*) \"([^\"]*)\" \"([^\"]*)\" ";
	//String regex = "([^ ]*) ([^ ]*) ([^ ]*) (\\[.*\\]) (\".*?\") (-|[0-9]*) (-|[0-9]*) (\".*?\") (\".*?\")";
	
	Pattern pattern = Pattern.compile(regex);	
			
	Matcher matcher = pattern.matcher(s);
	
	//Matcher matcher1 = pattern.matcher(s1);
	
		if(matcher.find())
		{
			for (int i = 1; i <= matcher.groupCount(); i++) 
			{
				System.out.println(matcher.group(i));
			}
		}else
		{
			System.out.println("not found");
		}
	
		//System.out.println("hello world");
	
	
	
	}
}
