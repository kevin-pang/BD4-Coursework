package Qn3;
import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer3_1 extends Reducer<CompositeKey3, LongWritable, LongWritable, Text> {

	@Override
	protected void reduce(CompositeKey3 key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		String outputValue = null;
		
		// foreach rev_id of the article
		for (LongWritable v : values) {	
			
			// Create a string of the latest revision and break out of loop
			outputValue = v + " " + getDateTimeString(key.getTimestamp());
			break;
		}	
		if(outputValue != null){
			// output (<article_id>, <rev_id> <timestamp>)
			context.write(new LongWritable(key.getArticle_ID()), new Text(outputValue));
		}
	}

	private static String getDateTimeString(long dt){
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(dt);
		return DatatypeConverter.printDateTime(cal);

	}

}
