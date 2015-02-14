package Qn2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator2 extends WritableComparator {
	protected CompositeKeyComparator2() {
		super(CompositeKey2.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey2 key1 = (CompositeKey2) w1;
		CompositeKey2 key2 = (CompositeKey2) w2;

		// (first check on getRev_ID_Count)
		int compare = Long.compare(key2.getRev_ID_Count(), key1.getRev_ID_Count());

		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (Article_ID)			
			return Long.compare(key1.getArticle_ID(), key2.getArticle_ID());
		}

		return compare;
	}
}