package comp6251;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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
	private static int diskWrite=0;

	public WriterThread(byte[][] tuples, File file) {
		this.tuples = tuples;
		this.file = file;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		diskWrite++;
		try (FileOutputStream stream = new FileOutputStream(file, true);) {
			for (int i = 0; i < tuples.length; i++) {
				if (tuples[i] == null)
					break;
				stream.write(tuples[i]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		}
	}
	
	public int getDiskWrite() {
		return diskWrite;
	}

}
