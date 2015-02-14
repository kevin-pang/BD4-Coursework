package Qn1;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer1 extends Reducer<CompositeKey1, LongWritable, LongWritable, Text> {
//	LongArrayWritable rev_id_array;
//	List<LongWritable> rev_id_list;
	@Override
	protected void reduce(CompositeKey1 key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int count = 0;
//			rev_id_array = new LongArrayWritable();
//			rev_id_list = new ArrayList<>();
			
			String rev_id_string = "";
			
			// foreach rev_id of the article
			for (LongWritable v : values) {
				// rev_id_list.add(v);		
				
				// add to string
				rev_id_string += v + " ";
				
				// increase counter
				count++;
			}
			
			// remove extra space behind
			rev_id_string = removeLastChar(rev_id_string);
			
			// create text for output value
			String text = count + " " + rev_id_string;
			
			// output (<article_id>, <string of all rev_id>)
			context.write(new LongWritable(key.getArticle_ID()), new Text(text));
			
	}
	private String removeLastChar(String rev_id_string) {
		return rev_id_string.substring(0, rev_id_string.length()-1);
	}

}
