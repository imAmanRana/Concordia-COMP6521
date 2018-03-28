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

import join.Constants;

public class NestedJoin {

	Utils utils = new Utils();

	public void Join(String inputFile1, String inputFile2, String output_file) throws IOException {

		int output_counter = 0;

		byte[][] output = new byte[Constants.TUPPLE_FOR_JOINED_OUTPUT][];
		ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
		ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(inputFile2));
		WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));
		
		int lineSeparatorLength = System.lineSeparator().getBytes().length;

		ByteBuffer buffer1 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T1 * (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength));
		ByteBuffer buffer2 = ByteBuffer.allocateDirect(
				Constants.TUPPLES_IN_BUFFER_T2 * (Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength));

		byte[] receive1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
		byte[][] tuples1 = null;
		int counter1 = 0;
		while (inChannel1.read(buffer1) > 0) {

			buffer1.flip();
			tuples1 = new byte[buffer1.limit() / (Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength)][];

			while (buffer1.hasRemaining()) {
				buffer1.get(receive1);
				tuples1[counter1++] = receive1;
				receive1 = null;
				receive1 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T1 + lineSeparatorLength];
			}

			byte[] receive2 = new byte[Constants.TUPLE_SIZE_IN_BYTES_T2 + lineSeparatorLength];

			while (inChannel2.read(buffer2) > 0) {
				buffer2.flip();
				while (buffer2.hasRemaining()) {
					buffer2.get(receive2);

					for (int i = 0; i < tuples1.length; i++) {
						if (utils.compare(tuples1[i], receive2)) {
							if (output_counter != Constants.TUPPLE_FOR_JOINED_OUTPUT)
								{ output[output_counter++] = utils.combine(tuples1[i], receive2);}
							else {
								int counter = output_counter;
								ByteBuffer out_buffer = ByteBuffer.allocateDirect(
										Constants.TUPPLE_FOR_JOINED_OUTPUT * (Constants.TUPLE_SIZE_IN_BYTES_T1 +Constants.TUPLE_SIZE_IN_BYTES_T2-8+ lineSeparatorLength));		
								while(output_counter>0)
								{
								out_buffer.put(output[--output_counter]);
								}
								out_buffer.flip();
								outChannel.write(out_buffer);
								out_buffer.clear();
								output = null;
							}

						}
					}
				}
			}

		}
if(output!= null)
{
	ByteBuffer out_buffer = ByteBuffer.allocateDirect(
			Constants.TUPPLE_FOR_JOINED_OUTPUT * (Constants.TUPLE_SIZE_IN_BYTES_T1 +Constants.TUPLE_SIZE_IN_BYTES_T2-8+ lineSeparatorLength));		
	
	while(output_counter>0)
		{
		out_buffer.put(output[--output_counter]);
		}
	out_buffer.flip();
	int x =outChannel.write(out_buffer);
	System.out.println(x);
	out_buffer.clear();
	outChannel.close();
	//output = null;
}
	}

}
