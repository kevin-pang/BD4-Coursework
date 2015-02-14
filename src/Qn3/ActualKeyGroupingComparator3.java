package Qn3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator3 extends WritableComparator {

	protected ActualKeyGroupingComparator3() {

		super(CompositeKey3.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey3 key1 = (CompositeKey3) w1;
		CompositeKey3 key2 = (CompositeKey3) w2;

		// (check on getArticle_ID)
		return Long.compare(key2.getArticle_ID(),key1.getArticle_ID());
	}
}