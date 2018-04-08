package comp6251;
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
	private final int tupleSize;

	public ReaderThread(int startPoint, int recordsToRead, File file, byte[][] tuples,int tupleSize) {
		this.startPoint = startPoint;
		this.recordsToRead = recordsToRead;
		this.file = file;
		this.tuples = tuples;
		this.tupleSize = tupleSize;
	}

	@Override
	public void run() {
		int lineSize = tupleSize + Constants.LINE_SEPARATOR_LENGTH;
		try (FileInputStream in = new FileInputStream(file); ReadableByteChannel inChannel = Channels.newChannel(in);) {

			
			in.skip(startPoint*lineSize);

			ByteBuffer buffer = ByteBuffer
					.allocateDirect(recordsToRead * (tupleSize + Constants.LINE_SEPARATOR_LENGTH));

			byte[] receive = new byte[lineSize];
			int counter = 0;
			while (inChannel.read(buffer) > 0) {

				buffer.flip();
				// read buffer
				while (buffer.hasRemaining()) {
					buffer.get(receive);
					tuples[counter++] = receive;
					receive = null;
					receive = new byte[tupleSize + Constants.LINE_SEPARATOR_LENGTH];
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
