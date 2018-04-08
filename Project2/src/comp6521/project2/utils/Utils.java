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
		return (int) ((double) file.length() / tupleSize);
	}

	public static int getIntegerData(byte[] record, int start, int end) {

		String s = new String(record, start, end).trim();
		if (s.isEmpty()) {
			return 0;
		} else {
			return Integer.parseInt(s);
		}
	}

	public static String getStringData(byte[] record, int start, int end) {

		String s = new String(record, start, end);
		return s.trim();
	}

	public static float gradeToMarks(String grade) {
		float marks = 0;
		switch (grade) {
		case "A+":
			marks = 4.3f;
			break;
		case "A":
			marks = 4.0f;
			break;
		case "A-":
			marks = 3.7f;
			break;
		case "B+":
			marks = 3.3f;
			break;
		case "B":
			marks = 0.0f;
			break;
		case "B-":
			marks = 2.7f;
			break;
		case "C+":
			marks = 2.3f;
			break;
		case "C":
			marks = 2.0f;
			break;
		case "C-":
			marks = 1.7f;
			break;
		case "D+":
			marks = 1.3f;
			break;
		case "D":
			marks = 1.0f;
			break;
		case "D-":
			marks = 0.7f;
			break;
		default:
			marks = 0.0f;
			break;
		}
		return marks;
	}

	public static ByteBuffer convertToBuffer(int studentId, byte[] gpa) {
		ByteBuffer buffer = ByteBuffer.allocate(Constants.STUDENT_ID_LENGTH + 5 + Constants.LINE_SEPARATOR_LENGTH)
				.put((studentId+" ").getBytes())
				.put(gpa)
				.put(System.lineSeparator().getBytes());
		buffer.flip();
		return buffer;
	}
}
