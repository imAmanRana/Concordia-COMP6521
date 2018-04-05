package comp6521.project2;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import comp6521.project2.utils.ByteArrayComparator;
import comp6521.project2.utils.Constants;
import comp6521.project2.utils.Utils;

public class SortedJoin {

	ByteArrayComparator bac = new ByteArrayComparator();

	public void sortJoin(String inputFile1, String inputFile2, String output_file) throws IOException {

		System.out.println(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN + "  "
				+ Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN + "  " + Constants.TUPPLE_FOR_JOINED_OUTPUT);

		int noOfRecordsInFile1 = Utils.findRecordsInFile(inputFile1,
				(Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.LINE_SEPARATOR_LENGTH));
		
		System.out.println("Records in file1 : "+noOfRecordsInFile1);

		ByteBuffer temp = null;

		try (ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
				ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(inputFile2));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));) {

			ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN
					* (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN
					* (Constants.TUPLE_SIZE_IN_BYTES_T2 + Constants.LINE_SEPARATOR_LENGTH));

			ByteBuffer outputBuffer = ByteBuffer.allocateDirect(Constants.TUPPLE_FOR_JOINED_OUTPUT
					* (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2 - Constants.STUDENT_ID_LENGTH
							+ Constants.LINE_SEPARATOR_LENGTH));

			byte[] record1;
			byte[] record2;

			int startPointer1 = 0;

			inChannel2.read(buffer2);

			buffer2.flip();

			record1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.LINE_SEPARATOR_LENGTH];

			record2 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T2 + Constants.LINE_SEPARATOR_LENGTH];
			buffer2.get(record2);

			while (startPointer1 < noOfRecordsInFile1) {
				buffer1.clear();
				inChannel1.read(buffer1);
				startPointer1 += (buffer1.position()
						/ (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.LINE_SEPARATOR_LENGTH));
				System.out.println("-> "+startPointer1);
				buffer1.flip();
				while (buffer1.hasRemaining()) {
					int value = bac.compare(record1, record2);

					if (value == 0) // ids are equal
					{
						if (outputBuffer.position() < outputBuffer.capacity()) {
							outputBuffer.put(Utils.combine(record1, record2));
						} else {
							
							temp = ByteBuffer
									.allocateDirect(Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2
											- Constants.STUDENT_ID_LENGTH + Constants.LINE_SEPARATOR_LENGTH);

							temp = Utils.combine(record1, record2);
							outputBuffer.flip();

							outChannel.write(outputBuffer);
							outputBuffer.clear();
							if(temp!=null) {
								outputBuffer.put(temp);
								temp=null;
							}
						}
						
						if (buffer2.hasRemaining()) {
							buffer2.get(record2);
						} // id in t1 is bigger
						else {
							buffer2.clear();
							inChannel2.read(buffer2);
							buffer2.flip();

						}
					} else if (value > 0) // id in t2 is bigger
					{
						if (buffer1.hasRemaining())
							buffer1.get(record1); // id in t1 is bigger
						else {
							buffer1.clear();
							inChannel1.read(buffer1);
							startPointer1 += (buffer1.position()
									/ (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.LINE_SEPARATOR_LENGTH));
							buffer1.flip();

						}
					} else {
						if (buffer2.hasRemaining())
							buffer2.get(record2); // id in t1 is bigger
						else {
							buffer2.clear();
							inChannel2.read(buffer2);
							buffer2.flip();
						}

					}

				}

			}
			
			if(buffer1.position() < buffer1.capacity()) {
				System.out.println("got here");
			}

			if (outputBuffer.position() > 0) {
				outputBuffer.flip();
				outChannel.write(outputBuffer);
				outputBuffer.clear();
			}
		}
	}

}
