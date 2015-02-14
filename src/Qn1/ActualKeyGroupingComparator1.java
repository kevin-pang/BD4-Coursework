package Qn1;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator1 extends WritableComparator {

	protected ActualKeyGroupingComparator1() {

		super(CompositeKey1.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey1 key1 = (CompositeKey1) w1;
		CompositeKey1 key2 = (CompositeKey1) w2;

		// (check on getArticle_ID)
		return Long.compare(key1.getArticle_ID(),key2.getArticle_ID());
		
	}
}