package Qn1;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator1 extends WritableComparator {
	protected CompositeKeyComparator1() {
		super(CompositeKey1.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey1 key1 = (CompositeKey1) w1;
		CompositeKey1 key2 = (CompositeKey1) w2;

		// (first check on Article_ID)
		//int compare = key1.getArticle_ID().compareTo(key2.getArticle_ID());
		int compare = Long.compare(key1.getArticle_ID(), key2.getArticle_ID());

		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (Rev_ID)
			//return key1.getRev_ID().compareTo(key2.getRev_ID());
			return Long.compare(key1.getRev_ID(), key2.getRev_ID());
		}

		return compare;
	}
}