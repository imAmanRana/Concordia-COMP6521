package comp6521.project2.utils;
import java.util.Comparator;

/**
 * Anti-Lexicographical byte array comparator
 * @author AmanRana
 *
 */
public class ByteArrayComparator implements Comparator<byte[]> {

	@Override
	public int compare(byte[] left, byte[] right) {
		
		if(left==null && right==null)
			return 0;
		else if (left == null && right != null)
			return 1;
		else if (left != null && right == null)
			return -1;
		else {

			for (int i = 0, j = 0; i < Constants.STUDENT_ID_LENGTH && j < Constants.STUDENT_ID_LENGTH; i++, j++) {
				int a = (left[i] & 0xff);
				int b = (right[j] & 0xff);
				if (a != b) {
					return b - a;
				}
			}
			return 0;
		}
	}

}
