package join;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * 
 */

/**
 * @author AmanRana
 *
 */
public class ReaderThread implements Runnable {

	private final int startPoint;
	private final int recordsToRead;
	private final File file;
	private final byte[][] tuples;
	private final int tuple_size;
	int lineSeparatorLength = System.lineSeparator().getBytes().length;

	public ReaderThread(int startPoint, int recordsToRead, File file,int tuple_size, byte[][] tuples) {
		this.startPoint = startPoint;
		this.recordsToRead = recordsToRead;
		this.file = file;
		this.tuples = tuples;
		this.tuple_size = tuple_size;
	}

	@Override
	public void run() {
		int lineSize = tuple_size + lineSeparatorLength;
		try (FileInputStream in = new FileInputStream(file); ReadableByteChannel inChannel = Channels.newChannel(in);) {

			
			in.skip(startPoint*lineSize);

			ByteBuffer buffer = ByteBuffer
					.allocateDirect(recordsToRead * (tuple_size + lineSeparatorLength));

			byte[] receive = new byte[lineSize];
			int counter = 0;
			while (inChannel.read(buffer) > 0) {

				buffer.flip();
				// read buffer
				while (buffer.hasRemaining()) {
					buffer.get(receive);
					tuples[counter++] = receive;
					receive = null;
					receive = new byte[tuple_size + lineSeparatorLength];
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
