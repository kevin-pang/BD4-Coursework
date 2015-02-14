package Qn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2_2 extends Reducer<CompositeKey2, LongWritable, LongWritable, LongWritable> {
	static enum Counters { NUM_COUNT }
	private int k;
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		// get conf object from context
		Configuration conf = context.getConfiguration();

		// set top k
		k = Integer.parseInt(conf.get("k"));
	}

	@Override
	protected void reduce(CompositeKey2 key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {	
		// foreach rev_id of the article
		for (LongWritable v : values) {			
			// check counter if count < k
			if(context.getCounter(Counters.NUM_COUNT).getValue() < k){
				// output (<article_id>, <rev_id_count>)
				context.write(v, new LongWritable (key.getRev_ID_Count()));
				context.getCounter(Counters.NUM_COUNT).increment(1);
			} else {
				break;
			}				
		}			
	}
}
