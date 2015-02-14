package Qn2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2_1 extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int count = 0;	
			
			// foreach rev_id of the article
			for (@SuppressWarnings("unused") LongWritable v : values) {				
				// increase counter
				count++;
			}		
		
			context.write(key, new IntWritable(count));
			
	}

}
