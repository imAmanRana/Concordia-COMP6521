package comp6521;
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
	int lineSeparatorLength = System.lineSeparator().getBytes().length;

	public ReaderThread(int startPoint, int recordsToRead, File file, byte[][] tuples) {
		this.startPoint = startPoint;
		this.recordsToRead = recordsToRead;
		this.file = file;
		this.tuples = tuples;
	}

	@Override
	public void run() {
		int lineSize = Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength;
		try (FileInputStream in = new FileInputStream(file); ReadableByteChannel inChannel = Channels.newChannel(in);) {

			
			in.skip(startPoint*lineSize);

			ByteBuffer buffer = ByteBuffer
					.allocateDirect(recordsToRead * (Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength));

			byte[] receive = new byte[lineSize];
			int counter = 0;
			while (inChannel.read(buffer) > 0) {

				buffer.flip();
				// read buffer
				while (buffer.hasRemaining()) {
					buffer.get(receive);
					tuples[counter++] = receive;
					receive = null;
					receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
