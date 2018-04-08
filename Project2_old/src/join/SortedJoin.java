package join;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import join.Constants;

public class SortedJoin {
	ByteArrayComparator bac = new ByteArrayComparator();

	public void sortJoin(String inputFile1, String inputFile2, String output_file) throws IOException {
		System.out.println(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN + "  "
				+ Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN + "  " + Constants.TUPPLE_FOR_JOINED_OUTPUT);
		
		File file1 = new File(inputFile1);
		

		int lineSeparatorLength = System.lineSeparator().getBytes().length;
		int noOfRecordsInFile1 = (int) (file1.length() / (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
		

		ByteBuffer temp = null;
		ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
		ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(inputFile2));
		WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));

		ByteBuffer buffer1 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN * (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
		ByteBuffer buffer2 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN * (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));

		ByteBuffer out_buffer = ByteBuffer.allocateDirect(Constants.TUPPLE_FOR_JOINED_OUTPUT
				* (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2 - 7));

		byte[] record1;
		byte[] record2;

		int startPointer1 = 0;
		int startPointer2 = 0;
				
		inChannel2.read(buffer2);		
		startPointer2 = startPointer2 + (buffer2.position() / (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));
		
		buffer2.flip();
		
		record1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
		buffer1.get(record1);
		record2 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength];
		buffer2.get(record2);
		int i = 0;

		while (startPointer1 < noOfRecordsInFile1 ) {
			buffer1.clear();
			inChannel1.read(buffer1);
			startPointer1 = startPointer1
					+ (buffer1.position() / (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
			buffer1.flip();

			
			while (buffer1.hasRemaining()) {
				byte[] studentID1 = Arrays.copyOfRange(record1, 0, 8);
				byte[] studentID2 = Arrays.copyOfRange(record2, 0, 8);
				int value = bac.compare(studentID1, studentID2);

				if (value == 0) // ids are equal
				{
					if (temp != null) {
						out_buffer.put(temp);
						temp = null;
					}
					if (out_buffer.position() < out_buffer.capacity()) {
						out_buffer.put(combine(record1, record2));
					} else {
						temp = ByteBuffer.allocateDirect(Constants.TUPLE_SIZE_IN_BYTES_T1
								+ Constants.TUPLE_SIZE_IN_BYTES_T2 - 8 + lineSeparatorLength);

						temp = combine(record1, record2);
						out_buffer.flip();
						outChannel.write(out_buffer);
						out_buffer.clear();
					}

					if (buffer2.hasRemaining()) {
						buffer2.get(record2);
					} // id in t1 is bigger
					else {
						buffer2.clear();
						inChannel2.read(buffer2);
						startPointer2 = startPointer2
								+ (buffer2.position() / (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));
						buffer2.flip();

					}
				} else if (value > 0) // id in t2 is bigger
				{
					if (buffer1.hasRemaining())
						buffer1.get(record1); // id in t1 is bigger
					else {
						buffer1.clear();
						inChannel1.read(buffer1);
						startPointer1 = startPointer1
								+ (buffer1.position() / (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
						buffer1.flip();

					}
				} else {
					if (buffer2.hasRemaining())
						buffer2.get(record2); // id in t1 is bigger
					else {
						buffer2.clear();
						inChannel2.read(buffer2);
						startPointer2 = startPointer2
								+ (buffer2.position() / (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));
						buffer2.flip();

					}

				}

			}
			
		}

		if (temp != null) {
			out_buffer.put(temp);
			temp = null;
		}	
		out_buffer.flip();
		outChannel.write(out_buffer);
		out_buffer.clear();
	}

	

	public ByteBuffer combine(byte[] tuple1, byte[] tuple2) {
		byte[] result = new byte[tuple1.length + tuple2.length - 9];
		for (int i = 0; i < tuple1.length - 1; i++) {
			result[i] = tuple1[i];
		}
		for (int j = 8; j < tuple2.length; j++)
			result[tuple1.length + j - 9] = tuple2[j];
		return ByteBuffer.wrap(result);
	}
}
