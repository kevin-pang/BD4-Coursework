package Qn2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper2_2 extends Mapper<LongWritable, Text, CompositeKey2, LongWritable> {
	private LongWritable article_id = new LongWritable();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer words = new StringTokenizer(line);

		if(words.hasMoreTokens()){
			// get values and store in a variable for reuse
			long article_id_long = getLongId(words.nextToken());
			long rev_id_count_long = getLongId(words.nextToken());
			
			// set value article_id LongWritable 
			article_id.set(article_id_long);
			
			// create CompositeKey ck to perform secondary sort
			CompositeKey2 ck = new CompositeKey2(rev_id_count_long,article_id_long);
			
			// switch count to key for sorting
			context.write(ck, article_id);
		}
	}

	private static long getLongId(String temp) {
		return Long.parseLong(temp);
	}

}
