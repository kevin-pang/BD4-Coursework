package Qn3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator3 extends WritableComparator {
	protected CompositeKeyComparator3() {
		super(CompositeKey3.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey3 key1 = (CompositeKey3) w1;
		CompositeKey3 key2 = (CompositeKey3) w2;

		// (first check on Article_ID)
		int compare = Long.compare(key1.getArticle_ID(), key2.getArticle_ID());


		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (Timestamp)
			return Long.compare(key2.getTimestamp(), key1.getTimestamp());
		}

		return compare;
	}
}