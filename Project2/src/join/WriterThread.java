package join;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 
 */

/**
 * @author AmanRana
 *
 */
public class WriterThread implements Runnable {

	private final byte[][] tuples;
	private final File file;
	private int size;
	private int tupleSize;
	
	public WriterThread(byte[][] tuples,File file,int size,int tupleSizeInBytes ) {
		this.tuples = tuples;
		this.file = file;
		this.size = size;
		this.tupleSize = tupleSizeInBytes;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try(FileChannel outChannel = new FileOutputStream(file,true).getChannel()){
			
			int lineSeparatorLength = System.lineSeparator().getBytes().length;
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					size * (tupleSize + lineSeparatorLength));

			// write the sorted buffer to file
			buffer.clear();
			int count=0;
			while (count < size) {
				if(tuples[count]==null)
					break;
				buffer.put(tuples[count++]);
			}
			buffer.flip();
			outChannel.write(buffer);
			outChannel.force(true);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
