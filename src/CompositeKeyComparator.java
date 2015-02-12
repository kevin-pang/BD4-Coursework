import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

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