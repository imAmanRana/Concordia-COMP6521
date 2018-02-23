import java.util.Comparator;

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

			for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
				int a = (left[i] & 0xff);
				int b = (right[j] & 0xff);
				if (a != b) {
					return b - a;
				}
			}
			return right.length - left.length;
		}
	}

}
