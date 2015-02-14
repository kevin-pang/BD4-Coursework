package Qn2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator2 extends WritableComparator {

	protected ActualKeyGroupingComparator2() {

		super(CompositeKey2.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey2 key1 = (CompositeKey2) w1;
		CompositeKey2 key2 = (CompositeKey2) w2;

		// (check on getArticle_ID)
		return Long.compare(key2.getArticle_ID(),key1.getArticle_ID());
		//return key1.getArticle_ID().compareTo(key2.getArticle_ID());
	}
}