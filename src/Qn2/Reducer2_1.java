package Qn2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2_1 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
			long count = 0;	
			
			// foreach count of the article
			for (IntWritable v : values) {				
				// add to counter
				count = count + v.get(); 
			}		
		
			context.write(key, new IntWritable((int) count));
			
	}

}
