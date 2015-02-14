package Qn3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKey3 implements WritableComparable<CompositeKey3> {
	private long timestamp;
	private long article_id;
	
	public CompositeKey3() { }
	
	public CompositeKey3(long timestamp, long article_id) {
		this.timestamp = timestamp;
		this.article_id = article_id;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder()).append(timestamp).append(',').append(article_id).toString();
	}
	 
	@Override
	public void readFields(DataInput in) throws IOException {
		article_id = WritableUtils.readVLong(in);
		timestamp = WritableUtils.readVLong(in);
		
	}
	 
	@Override
	public void write(DataOutput out) throws IOException { 
		WritableUtils.writeVLong(out, article_id);
		WritableUtils.writeVLong(out, timestamp);
		
	}
	
	@Override
	public int compareTo(CompositeKey3 ck) {
		int result = Long.compare(timestamp, ck.timestamp);
		if (0 == result) {
			result = Long.compare(article_id, ck.article_id);
		}
		return result;
	}

	/**
	* Gets the timestamp.
	*
	* @return timestamp.
	*/
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Gets the article_id.
	 * 
	 * @return article_id
	 */
	public long getArticle_ID() {
		return article_id;
	}

	public void setArticle_ID(long article_id) {
		this.article_id = article_id;
	}
}
