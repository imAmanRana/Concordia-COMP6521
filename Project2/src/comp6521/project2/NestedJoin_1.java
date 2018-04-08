package comp6521.project2;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import comp6521.project2.utils.Constants;

public class NestedJoin_1 {

	public void join(final String inputFile1, final String inputFile2, final String output_file) throws IOException {
		System.out.println(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN + "  "
				+ Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN + "  " + Constants.TUPPLE_FOR_JOINED_OUTPUT);

		int outputCounter = 0;
		int lineSeparatorLength = System.lineSeparator().getBytes().length;

		byte[] temp = null;
		byte[][] output = new byte[Constants.TUPPLE_FOR_JOINED_OUTPUT][];
		try (ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));) {
			
			ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN
					* (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
			ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN
					* (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));

			byte[] receive1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
			byte[][] tuples1 = null;

			while (inChannel1.read(buffer1) > 0) {
				
				int counter1 = 0;
				buffer1.flip();
				tuples1 = new byte[buffer1.limit() / (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength)][];
				while (buffer1.hasRemaining()) {
					buffer1.get(receive1);
					tuples1[counter1++] = receive1;
					receive1 = null;
					receive1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
				}
				buffer1.clear();
				byte[] receive2 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength];

				ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(inputFile2));
				while (inChannel2.read(buffer2) > 0) {
					buffer2.flip();
					while (buffer2.hasRemaining()) {
						buffer2.get(receive2);
						for (int i = 0; i < tuples1.length; i++) {
							if (compare(tuples1[i], receive2)) {
								if (temp != null) {
									output[outputCounter++] = temp;
									temp = null;
								}
								if (outputCounter != Constants.TUPPLE_FOR_JOINED_OUTPUT) {
									output[outputCounter++] = combine(tuples1[i], receive2);

								} else {
									temp = tuples1[i];
									ByteBuffer out_buffer = ByteBuffer.allocateDirect(
											Constants.TUPPLE_FOR_JOINED_OUTPUT * (Constants.TUPLE_SIZE_IN_BYTES_T1
													+ Constants.TUPLE_SIZE_IN_BYTES_T2 - 7));
									while (outputCounter > 0) {
										out_buffer.put(output[--outputCounter]);

									}
									out_buffer.flip();
									outChannel.write(out_buffer);
									out_buffer.clear();
									output = null;
									output = new byte[Constants.TUPPLE_FOR_JOINED_OUTPUT][];
									outputCounter = 0;
								}

							}
						}
					}
					buffer2.clear();
				}
				buffer1.clear();
				inChannel2.close();
			}
			inChannel1.close();
			if (temp != null) {
				output[outputCounter++] = temp;
				temp = null;
			}
			if (output != null) {
				ByteBuffer out_buffer = ByteBuffer
						.allocateDirect(Constants.TUPPLE_FOR_JOINED_OUTPUT * (Constants.TUPLE_SIZE_IN_BYTES_T1
								+ Constants.TUPLE_SIZE_IN_BYTES_T2 - 7 + lineSeparatorLength));

				while (outputCounter > 0) {
					out_buffer.put(output[--outputCounter]);

				}
				out_buffer.flip();
				outChannel.write(out_buffer);
				out_buffer.clear();
				outChannel.close();
				// output = null;

			}
		}

	}

	public byte[] combine(byte[] tuple1, byte[] tuple2) {

		byte[] result = new byte[tuple1.length + tuple2.length - 9];
		for (int i = 0; i < tuple1.length - 1; i++) {
			result[i] = tuple1[i];
		}
		for (int j = 8; j < tuple2.length; j++)
			result[tuple1.length + j - 9] = tuple2[j];

		return result;
	}

	public boolean compare(byte[] tuple1, byte[] tuple2) {
		for (int i = 0; i < 8; i++) {
			if (tuple1[i] != tuple2[i])
				return false;
		}
		return true;
	}
}
