import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper<LongWritable, Text, CompositeKey, LongWritable> {
	private LongWritable article_id = new LongWritable();
	private LongWritable rev_id = new LongWritable();
	private Date start;
	private Date end;

	// set up mapper
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		// get conf object from context
		Configuration conf = context.getConfiguration();
		
		// set start and end date
		start = getDateTime(conf.get("start"));
		end = getDateTime(conf.get("end"));
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		// if line start with "REVISION", else skip line
		if(getFirstWord(line).equals("REVISION")){
			// split remaining words 
			StringTokenizer words = new StringTokenizer(getRemainingString(line));
			
			// set article_id and rev_id LongWritable
			long article_id_long = getLongId(words.nextToken());
			long rev_id_long = getLongId(words.nextToken());
			//article_id.set(article_id_long);
			rev_id.set(rev_id_long);
			
			// get timestamp
			words.nextToken(); //to skip article_title
			Date timestamp = getDateTime(words.nextToken());
			
			CompositeKey ck = new CompositeKey();
			ck.setArticle_ID(article_id_long);
			ck.setRev_ID(rev_id_long);
			
			// if timestamp is within start and end date add
			if(checkDateWithin(timestamp)){			
				context.write(ck, rev_id);
			}
			
		}
	}

	private boolean checkDateWithin(Date timestamp){
		if(checkDateExist()){
			return timestamp.after(start) && timestamp.before(end) || timestamp.equals(start) || timestamp.equals(end);
		}
		else {
			return false;
		}
	}


private boolean checkDateExist() {
	return start != null && end != null;
}

private static Date getDateTime(String dtString){
	return DatatypeConverter.parseDateTime(dtString).getTime();
}

private static long getLongId(String temp) {
	return Long.parseLong(temp);
}

private static String getFirstWord(String text) {
	String substring = "";
	try{
		substring = text.substring(0, text.indexOf(' '));
	}catch (IndexOutOfBoundsException e){


	}catch (Exception ex){
		ex.printStackTrace();
	}
	return substring;
}

private static String getRemainingString(String text) {
	return text.substring(text.indexOf(' '), text.length()).trim();
}


}
