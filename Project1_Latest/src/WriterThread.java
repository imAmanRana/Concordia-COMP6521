import java.io.File;
import java.io.FileNotFoundException;
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
	
	public WriterThread(byte[][] tuples,File file,int size) {
		this.tuples = tuples;
		this.file = file;
		this.size = size;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try(FileChannel outChannel = new FileOutputStream(file,true).getChannel()){
			
			int lineSeparatorLength = System.lineSeparator().getBytes().length;
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					size * (Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength));

			// write the sorted buffer to file
			buffer.clear();
			int count=0;
			while (count < size) {
				if(tuples[count]==null)
					continue;
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
