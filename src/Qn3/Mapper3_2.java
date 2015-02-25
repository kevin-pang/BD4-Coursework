package Qn3;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper3_2 extends Mapper<LongWritable, Text, CompositeKey3, LongWritable> {
	private LongWritable rev_id = new LongWritable();


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {		

		StringTokenizer words = new StringTokenizer(value.toString());

		if(words.hasMoreTokens()){
			// get values and store in a variable for reuse
			long timestamp_long = getLongId(words.nextToken());
			long rev_id_long = getLongId(words.nextToken());

			// set value rev_id LongWritable 
			rev_id.set(rev_id_long);

			// create CompositeKey ck to perform secondary sort
			CompositeKey3 ck = new CompositeKey3(timestamp_long, key.get());

			// Key: article_id, timestamp Value: article_id
			context.write(ck, rev_id);			

		}
	}
	

	private static long getLongId(String temp) {
		return Long.parseLong(temp);
	}

	


}
