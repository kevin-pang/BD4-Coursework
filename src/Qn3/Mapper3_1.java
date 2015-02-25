package Qn3;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3_1 extends Mapper<LongWritable, Text, LongWritable, Text> {
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();
	private Date timestampInput;
	private String valueString;
	// set up mapper
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		// get conf object from context
		Configuration conf = context.getConfiguration();
		// set start and end date
		timestampInput = getDateTime(conf.get("timestamp"));
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		// if line start with "REVISION", else skip line
		if(getFirstWord(line).equals("REVISION")){
			// split remaining words 
			StringTokenizer words = new StringTokenizer(getRemainingString(line));

			// get values and store in a variable for reuse
			long article_id_long = getLongId(words.nextToken());
			long rev_id_long = getLongId(words.nextToken());

			

			// get timestamp
			words.nextToken(); //to skip article_title
			Date timestamp = getDateTime(words.nextToken());

			// if timestamp =< timestampInput 
			if(checkDateBefore(timestamp)){		

				// create CompositeKey ck to perform secondary sort
				//CompositeKey3 ck = new CompositeKey3(timestamp.getTime(), article_id_long);
				
				// set articleId as key
				outKey.set(article_id_long);
				
				// set value date + rev_id as Text 
				valueString = String.format("%d %d", timestamp.getTime(), rev_id_long);
				outValue.set(valueString);
				
				// Key: article_id  Value: timestamp, rev_id
				context.write(outKey, outValue);

			}
		}
	}

	private boolean checkDateBefore(Date timestamp){
		if(checkDateExist()){
			return timestamp.before(timestampInput) || timestamp.equals(timestampInput);
		}
		else {
			return false;
		}
	}


	private boolean checkDateExist() {
		return timestampInput != null;
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
