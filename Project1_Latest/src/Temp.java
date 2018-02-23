import java.util.Arrays;

public class Temp {

	public static void main(String[] args) {
		byte[] aman = {56,12,12,34,32,12,32,0,74};
		byte[][] t1 = new byte[3][];
		int counter=0;
		t1[counter++] = aman;
		t1[counter++] = new byte[]{56,12,58,21};
		t1[counter++] = new byte[]{56,4,50,28};
		
		for(byte[] a : t1) {
			for(int i=0;i<a.length;i++) {
				System.out.print(a[i]+" ");
			}
			System.out.println();
		}
		
		
		Arrays.sort(t1,new ByteArrayComparator());
		
		System.out.println();
		for(byte[] a : t1) {
			for(int i=0;i<a.length;i++) {
				System.out.print(a[i]+" ");
			}
			System.out.println();
		}
	}

}
