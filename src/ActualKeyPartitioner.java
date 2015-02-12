import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ActualKeyPartitioner extends Partitioner<CompositeKey, LongWritable> {

	HashPartitioner<LongWritable, LongWritable> hashPartitioner = new HashPartitioner<LongWritable, LongWritable>();
	LongWritable newKey = new LongWritable();
	
	@Override
	public int getPartition(CompositeKey key, LongWritable value, int numReduceTasks) {

		try {
			// Execute the default partitioner over the first part of the key
			newKey.set(key.getArticle_ID());
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks); // this would return a random value in the range
			// [0,numReduceTasks)
		}
	}
}