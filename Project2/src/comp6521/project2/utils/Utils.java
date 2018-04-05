package comp6521.project2.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

public class Utils {

	private Utils() {
	}

	public static boolean compare(final byte[] tuple1, final byte[] tuple2) {
		for (int i = 0; i < 8; i++) {
			if (tuple1[i] != tuple2[i])
				return false;
		}
		return true;
	}
	
	public static boolean isByteArrayEmpty(byte[] record) {
		for (byte b : record) {
		    if (b != 0) {
		        return false;
		    }
		}
		return true;
	}

	public static ByteBuffer combine(final byte[] tuple1, final byte[] tuple2) {
		ByteBuffer buffer = ByteBuffer
				.allocate(tuple1.length + tuple2.length - Constants.STUDENT_ID_LENGTH - Constants.LINE_SEPARATOR_LENGTH)
				.put(tuple1, 0, tuple1.length - Constants.LINE_SEPARATOR_LENGTH)
				.put(tuple2, Constants.STUDENT_ID_LENGTH, tuple2.length - Constants.STUDENT_ID_LENGTH);

		buffer.flip();
		return buffer;

	}

	public static boolean createNewFile(File file) throws IOException {

		if (!file.exists()) {
			return file.createNewFile();
		}
		return false;
	}

	public static void clearFile(File file) throws IOException {

		boolean status = createNewFile(file);
		if (!status) {
			new PrintWriter(file).close();
		}
	}

	public static int findRecordsInFile(final String fileName, final int tupleSize) {
		
		File file = new File(fileName);
		return (int) ((double)file.length() / tupleSize);
	}

}
