package Qn1;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ActualKeyPartitioner1 extends Partitioner<CompositeKey1, LongWritable> implements Configurable{

	private Configuration conf;
	private int largestId;
	
	@Override
	public int getPartition(CompositeKey1 key, LongWritable value, int numReduceTasks) {
		
		try {
			// Execute the custom partitioner over the first part of the key			
			// assign reducer based on range
			return (int) (key.getArticle_ID()/(largestId/numReduceTasks+1));
			
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks); // this would return a random value in the range
			// [0,numReduceTasks)
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		// get largestId from conf
		largestId = conf.getInt("largestId", 0);
		
	}
}