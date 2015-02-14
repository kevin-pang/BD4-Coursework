package Qn2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKey2 implements WritableComparable<CompositeKey2> {
	private long rev_id_count;
	private long article_id;
	
	public CompositeKey2() { }
	
	public CompositeKey2(long rev_id_count, long article_id) {
		this.rev_id_count = rev_id_count;
		this.article_id = article_id;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder()).append(rev_id_count).append(',').append(article_id).toString();
	}
	 
	@Override
	public void readFields(DataInput in) throws IOException {
//		rev_id_count = WritableUtils.readVLong(in);
//		article_id = WritableUtils.readVLong(in);
		article_id = WritableUtils.readVLong(in);
		rev_id_count = WritableUtils.readVLong(in);
		
	}
	 
	@Override
	public void write(DataOutput out) throws IOException { 
//		WritableUtils.writeVLong(out, rev_id_count);
//		WritableUtils.writeVLong(out, article_id);
		WritableUtils.writeVLong(out, article_id);
		WritableUtils.writeVLong(out, rev_id_count);
		
	}
	
	@Override
	public int compareTo(CompositeKey2 ck) {
		//CompositeKey ck = (CompositeKey) o;
		//int result = article_id.compareTo(ck.article_id);
		int result = Long.compare(rev_id_count, ck.rev_id_count);
		if (0 == result) {
			result = Long.compare(article_id, ck.article_id);
			//result = rev_id.compareTo(ck.rev_id);
		}
		return result;
	}

	/**
	* Gets the article_id.
	*
	* @return article_id.
	*/
	public long getRev_ID_Count() {
		return rev_id_count;
	}

	public void setRev_ID_Count(long rev_id_count) {
		this.rev_id_count = rev_id_count;
	}

	/**
	 * Gets the rev_id.
	 * 
	 * @return rev_id
	 */
	public long getArticle_ID() {
		return article_id;
	}

	public void setArticle_ID(long article_id) {
		this.article_id = article_id;
	}
}
