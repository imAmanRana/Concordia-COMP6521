import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 
 */

/**
 * @author AmanRana
 *
 */
public class Main {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			// TODO - update the output for user
			System.out.println("Please provide valid input");
		} else {
			switch (args[0]) {

			case "1":
				// clearFile(new File(Constants.OUTPUT_FILE1));
				clearFile(new File(Constants.MERGED_OUTPUT1));
				// do sorting for bag1
				sortAndMerge(new File(Constants.INPUT_FILE1), new File(Constants.OUTPUT_FILE1),
						new File(Constants.MERGED_OUTPUT1));
				break;
			case "2":
				clearFile(new File(Constants.OUTPUT_FILE2));
				clearFile(new File(Constants.MERGED_OUTPUT2));
				// do sorting for bag1
				sortAndMerge(new File(Constants.INPUT_FILE2), new File(Constants.OUTPUT_FILE2),
						new File(Constants.MERGED_OUTPUT2));
				break;
			case "3":
				break;
			default:
				System.out.println("Please provide valid input");
				break;

			}
		}

	}

	/**
	 * Creates new file if it doesn't exists.
	 * 
	 * @param file
	 * @return true if the file exists or was successfully created.
	 * @throws IOException
	 */
	public static boolean createNewFile(File file) throws IOException {

		if (!file.exists()) {
			return file.createNewFile();
		}
		return false;
	}

	/**
	 * Clears the content of the {@code file}, if the file doesn't exists, it
	 * creates a new one
	 * 
	 * @param file
	 *            file to clear
	 * @throws IOException
	 */
	public static void clearFile(File file) throws IOException {

		boolean status = createNewFile(file);
		if (!status) {
			new PrintWriter(file).close();
		}

	}

	private static void sortAndMerge(File inputFile, File outputFile, File intermediateFile) throws IOException {

		// sublist sorting file 1
		long start = System.nanoTime();
		sortSublist(inputFile, outputFile);
		long end = System.nanoTime();
		System.out.println("Sublist sort : " + (end - start) / 1_000_000_000 + " seconds");

		start = System.nanoTime();
		mergeSublist(outputFile, intermediateFile);
		end = System.nanoTime();
		System.out.println("Merging : " + (end - start) / 1_000_000_000 + " seconds");

	}

	private static void mergeSublist(File inputFile, File outputFile) {

		int noOfSublists = (int) Math.ceil((double) inputFile.length() / (100 * Constants.TUPPLES_IN_BUFFER));
		int[] recordsFetched = new int[noOfSublists];
		int[] startPoint = new int[noOfSublists];
		int recordsToRead = (int) ((double) Constants.TUPPLES_IN_BUFFER / (noOfSublists * 2));
		recordsToRead = recordsToRead == 0 ? 1 : recordsToRead;

		for (int i = 0; i < noOfSublists; i++) {
			startPoint[i] = i * Constants.TUPPLES_IN_BUFFER;
		}

		byte[][][] tuples = new byte[noOfSublists][][];
		// read parallely from all sublists
		{
			ExecutorService executor = Executors.newFixedThreadPool(noOfSublists);
			for (int i = 0; i < noOfSublists; i++) {
				tuples[i] = new byte[recordsToRead][];
				executor.execute(
						new ReaderThread(startPoint[i], recordsFetched[i], recordsToRead, inputFile, tuples[i]));
				recordsFetched[i] += recordsToRead;
				startPoint[i] += recordsToRead;
			}
			executor.shutdown();
			// Wait until all threads are finish
			while (!executor.isTerminated()) {

			}
			executor = null;
		}

		{
			byte[] min;
			int minList = -1;
			int count = 0;
			int aman = 1;
			int[] currentReadPointer = new int[noOfSublists];
			byte[][] sorted = new byte[recordsToRead][];
			boolean[] allRecordsFetched = new boolean[noOfSublists];
			int currentOutputPointer = 0;
			boolean allTuplesRead = false;
			ByteArrayComparator bac = new ByteArrayComparator();
			outer: while (!allTuplesRead) {
				int j = -1;
				aman++;
				min = new byte[] { Byte.MAX_VALUE };
				minList = -1;
				for (int i = 0; i < noOfSublists; i++) {
					j = -1;
					if (allRecordsFetched[i]) {
						continue;
					} else if (recordsToRead - currentReadPointer[i] > 0 || currentReadPointer[i] < tuples[i].length) {
						j = currentReadPointer[i];
					} else if (!allRecordsFetched[i] && recordsFetched[i] == Constants.TUPPLES_IN_BUFFER
							&& currentReadPointer[i] == tuples[i].length) {
						allRecordsFetched[i] = true;
						count++;
					} else {
						j = 0;
						currentReadPointer[i] = 0;
						Runnable task = new ReaderThread(startPoint[i], recordsFetched[i], recordsToRead, inputFile,
								tuples[i]);
						recordsFetched[i] += recordsToRead;
						startPoint[i] += recordsToRead;
						Thread t = new Thread(task);
						t.start();
						// try {
						// t.join();
						// } catch (InterruptedException e) {
						// e.printStackTrace();
						// }

					}

					if (count == noOfSublists) {
						allTuplesRead = true;
						break outer;
					} else if (j != -1 && tuples[i] != null && tuples[i][j] != null
							&& bac.compare(tuples[i][j], min) > 0) {

						min = tuples[i][j];
						minList = i;
					}
				}
				// {
				// //System.out.println(aman);
				// String tem =new String(min);
				// System.out.print(tem.trim()+" : "+tem.length()+" : "+minList);
				// System.out.println(" : currentReadPointer : "+currentReadPointer[minList]+" :
				// startPoint "+startPoint[minList]);
				// }
				if (minList == -1) {
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead));
					t.start();
					// try {
					// t.join();
					// } catch (InterruptedException e) {
					// e.printStackTrace();
					// }
					break;
				}
				currentReadPointer[minList] += 1;
				if (currentOutputPointer == recordsToRead) {
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead));
					t.start();
					try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					currentOutputPointer = 0;
					sorted[currentOutputPointer++] = min;
				} else {
					sorted[currentOutputPointer++] = min;
				}
			}

		}

	}

	private static void sortSublist(File inputFile, File outputFile) throws IOException {
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(inputFile));
				FileChannel outChannel = new FileOutputStream(outputFile).getChannel()) {
			int lineSeparatorLength = System.lineSeparator().getBytes().length;
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					Constants.TUPPLES_IN_BUFFER * (Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength));

			byte[] receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];

			byte[][] tuples = new byte[Constants.TUPPLES_IN_BUFFER][];
			int counter = 0;
			int sublists = 0;
			while (inChannel.read(buffer) > 0) {

				buffer.flip();
				// read buffer
				while (buffer.hasRemaining()) {
					buffer.get(receive);
					tuples[counter++] = receive;
					receive = null;
					receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];
				}

				if (tuples.length == Constants.TUPPLES_IN_BUFFER) {
					// sort the buffer
					Arrays.sort(tuples, new ByteArrayComparator());

					// write the sorted buffer to file
					buffer.clear();
					while (counter > 0) {
						buffer.put(tuples[--counter]);
					}
					buffer.flip();
					outChannel.write(buffer);
					buffer.clear();
					tuples = null;
					tuples = new byte[Constants.TUPPLES_IN_BUFFER][];
					sublists++;
				}

			}
			System.out.println("no of sublists : " + sublists + "\n TUPPLES IN BUFFER " + Constants.TUPPLES_IN_BUFFER);
		}
	}

	private static void showStats(String where, FileChannel fc, Buffer b) throws IOException {
		System.out.println(where + " channelPosition: " + fc.position() + " bufferPosition: " + b.position()
				+ " limit: " + b.limit() + " remaining: " + b.remaining() + " capacity: " + b.capacity());
	}

	private static void showStats(String where, Buffer b) throws IOException {
		System.out.println(where + " bufferPosition: " + b.position() + " limit: " + b.limit() + " remaining: "
				+ b.remaining() + " capacity: " + b.capacity());
	}

	private static long getSeekPosition(long recordsToRead) {
		return (recordsToRead == 1 ? 0
				: ((recordsToRead - 1) * (Constants.TUPLE_SIZE_IN_BYTES + System.lineSeparator().getBytes().length)));
	}

}
