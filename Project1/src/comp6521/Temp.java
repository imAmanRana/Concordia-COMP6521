package comp6521;
import java.util.Arrays;

public class Temp {

	public static void main(String[] args) {
		byte[] aman = { 56, 12, 12, 34, 32, 12, 32, 0, 74 };
		byte[][] t1 = new byte[3][];
		int counter = 0;
		t1[counter++] = aman;
		t1[counter++] = new byte[] { 56, 12, 58, 21 };
		t1[counter++] = new byte[] { 56, 4, 50, 28 };

		for (byte[] a : t1) {
			for (int i = 0; i < a.length; i++) {
				System.out.print(a[i] + " ");
			}
			System.out.println();
		}

		Arrays.sort(t1, new ByteArrayComparator());

		System.out.println();
		for (byte[] a : t1) {
			for (int i = 0; i < a.length; i++) {
				System.out.print(a[i] + " ");
			}
			System.out.println();
		}

		System.out.println("\n\nAMAN");
		byte[] record = ("Aman                    ").getBytes();
		String template = "%s:%-3d"+System.lineSeparator();
		int diff = 13;
		System.out.print(String.format(template, new String(record),diff>0?diff:0));
	}

}
