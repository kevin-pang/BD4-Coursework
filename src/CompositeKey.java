import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKey implements WritableComparable {
	private long article_id;
	private long rev_id;
	
	public CompositeKey() { }
	
	public CompositeKey(long article_id, long rev_id) {
		this.article_id = article_id;
		this.rev_id = rev_id;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder()).append(article_id).append(',').append(rev_id).toString();
	}
	 
	@Override
	public void readFields(DataInput in) throws IOException {
		article_id = WritableUtils.readVLong(in);
		rev_id = WritableUtils.readVLong(in);
	}
	 
	@Override
	public void write(DataOutput out) throws IOException { 
		WritableUtils.writeVLong(out, article_id);
		WritableUtils.writeVLong(out, rev_id);
	}
	
	@Override
	public int compareTo(Object o) {
		CompositeKey ck = (CompositeKey) o;
		//int result = article_id.compareTo(ck.article_id);
		int result = Long.compare(article_id, ck.article_id);
		if (0 == result) {
			result = Long.compare(rev_id, ck.rev_id);
			//result = rev_id.compareTo(ck.rev_id);
		}
		return result;
	}

	/**
	* Gets the article_id.
	*
	* @return article_id.
	*/
	public long getArticle_ID() {
		return article_id;
	}

	public void setArticle_ID(long article_id) {
		this.article_id = article_id;
	}

	/**
	 * Gets the rev_id.
	 * 
	 * @return rev_id
	 */
	public long getRev_ID() {
		return rev_id;
	}

	public void setRev_ID(long rev_id) {
		this.rev_id = rev_id;
	}
}
