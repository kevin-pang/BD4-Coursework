import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper2_2 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	private LongWritable article_id = new LongWritable();
	private LongWritable rev_id_count = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer words = new StringTokenizer(line);

		if(words.hasMoreTokens()){
			article_id.set(getLongId(words.nextToken()));
			rev_id_count.set(getIntId(words.nextToken()));

			// switch count to key for sorting
			context.write(rev_id_count, article_id);
		}
	}

	private static long getLongId(String temp) {
		return Long.parseLong(temp);
	}

	private static int getIntId(String temp) {
		return Integer.parseInt(temp);
	}

}
