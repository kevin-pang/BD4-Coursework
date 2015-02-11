import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2_2 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {					
			
			// foreach rev_id of the article
			for (LongWritable v : values) {
				// output (<article_id>, <rev_id_count>)
				context.write(v, key);
			}			
	}
}
