package comp6521;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

/**
 * 
 */

/**
 * @author AmanRana
 *
 */
public class SearchDuplicates {

	static int readPointer;
	static int startPointer;
	static int count;
	static byte[][] tuple;
	static File file = new File(Constants.MERGED_OUTPUT2);
	static ByteArrayComparator bac = new ByteArrayComparator();

	private SearchDuplicates() {

	}

	public static int findDuplicatesInFile(byte[] record) {

		int duplicates = 0;
		if (tuple == null || readPointer <= startPointer) {
			tuple = new byte[Constants.TUPPLES_IN_BUFFER][];
			readData(tuple);
			count=0;

		}

		while (tuple[count] != null && bac.compare(record, tuple[count]) <= 0) {
			
			if(bac.compare(record, tuple[count])<0) {
				//do nothing
			}else {
				duplicates++;
			}
			count++;
			startPointer++;
			if (readPointer <= startPointer) {
				tuple = new byte[Constants.TUPPLES_IN_BUFFER][];
				readData(tuple);
				count=0;
			}
		}
		
		return duplicates;

	}

	private static void readData(byte[][] tuple) {
		Runnable task = new ReaderThread(readPointer,Constants.TUPPLES_IN_BUFFER ,file,tuple);
		readPointer+=Constants.TUPPLES_IN_BUFFER;
		Thread t = new Thread(task);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
