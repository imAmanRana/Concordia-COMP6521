package join;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import join.Constants;

public class NestedJoin2 {
	public void Join(String inputFile1, String inputFile2, String output_file) throws IOException {
		System.out.println(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN + "  "
				+ Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN + "  " + Constants.TUPPLE_FOR_JOINED_OUTPUT);
		int output_counter = 0;
		int lineSeparatorLength = System.lineSeparator().getBytes().length;

		ByteBuffer temp = ByteBuffer.allocateDirect(
				Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2 - 8 + lineSeparatorLength);
		temp = null;

		ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
		WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));

		ByteBuffer buffer1 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN * (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
		ByteBuffer buffer2 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN * (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));

		ByteBuffer out_buffer = ByteBuffer.allocateDirect(Constants.TUPPLE_FOR_JOINED_OUTPUT
				* (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2 - 7));

		while (inChannel1.read(buffer1) > 0) {
			buffer1.flip();
			ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(inputFile2));

			while (inChannel2.read(buffer2) > 0) {
				buffer2.flip();

				while (buffer2.hasRemaining()) {
					byte[] receive2 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength];
					buffer2.get(receive2);
					
					while (buffer1.hasRemaining()) {
						byte[] receive1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
						buffer1.get(receive1);

						if (compare(receive1, receive2)) {
							if (temp != null) {
								out_buffer.put(temp);
								temp = null;
							}
							if (output_counter != Constants.TUPPLE_FOR_JOINED_OUTPUT) {
								out_buffer.put(combine(receive1, receive2));
								output_counter++;							
							} else {
								
								temp = combine(receive1, receive2);
								
								out_buffer.flip();
								Thread t =new Thread( new Runnable()
								{
									public void run()
									{
								try {
									outChannel.write(out_buffer);
								
								out_buffer.clear();
								
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
									}
								});
								t.start();
								output_counter = 0;
							}
						}
					}
					buffer1.flip();
				}
				buffer2.clear();
			}
			buffer1.clear();
			inChannel2.close();

		}
		inChannel1.close();
		if (temp != null) {
			out_buffer.put(temp);
			temp = null;
		}
		if (output_counter != 0) {
			out_buffer.flip();
			outChannel.write(out_buffer);
			out_buffer.clear();
			outChannel.close();
		}

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

	public boolean compare(byte[] tuple1, byte[] tuple2) {
		for (int i = 0; i < 8; i++) {
			if (tuple1[i] != tuple2[i])
				return false;
		}
		return true;
	}
}
