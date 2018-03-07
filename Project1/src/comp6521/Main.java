package comp6521;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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

	static ByteArrayComparator bac = new ByteArrayComparator();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			printHelp();
		} else {
			switch (args[0]) {

			case "1":
				clearFile(new File(Constants.OUTPUT_FILE1));
				clearFile(new File(Constants.MERGED_OUTPUT1));
				// do sorting and merging for bag1
				System.out.println("BAG1");
				sortAndMerge(new File(Constants.INPUT_FILE1), new File(Constants.OUTPUT_FILE1),
						new File(Constants.MERGED_OUTPUT1));
				break;
			case "2":
				clearFile(new File(Constants.OUTPUT_FILE2));
				clearFile(new File(Constants.MERGED_OUTPUT2));
				// do sorting and merging for bag2
				System.out.println("BAG2");
				sortAndMerge(new File(Constants.INPUT_FILE2), new File(Constants.OUTPUT_FILE2),
						new File(Constants.MERGED_OUTPUT2));
				break;
			case "3":
				clearFile(new File(Constants.FINAL_OUTPUT));
				long start = System.nanoTime();
				bagDifference(new File(Constants.MERGED_OUTPUT1), new File(Constants.FINAL_OUTPUT));
				long end = System.nanoTime();
				System.out.println("Bag Difference : " + (end - start) / 1_000_000_000 + " seconds");

				break;
			default:
				printHelp();
				break;

			}
		}

	}

	public static void printHelp() {
		System.out.println("Please provide a valid command line input");
		System.out.println("1 - Sorting and Merging for :" + Constants.INPUT_FILE1);
		System.out.println("2 - Sorting and Merging for :" + Constants.INPUT_FILE2);
		System.out.println("3 - Bag Difference for  :" + Constants.INPUT_FILE1 + " & " + Constants.INPUT_FILE2);
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

	/**
	 * Performs sublist sorting and then merging of the input file
	 * 
	 * @param inputFile
	 * @param outputFile
	 * @param intermediateFile
	 * @throws IOException
	 */
	private static void sortAndMerge(File inputFile, File outputFile, File intermediateFile) throws IOException {

		// sublist sorting file 1
		long start = System.nanoTime();
		sortSublist(inputFile, outputFile);
		long end = System.nanoTime();
		System.out.println("Sublist Sort : " + (end - start) / 1_000_000_000 + " seconds");

		start = System.nanoTime();
		mergeSublist(outputFile, intermediateFile);
		end = System.nanoTime();
		System.out.println("Merging : " + (end - start) / 1_000_000_000 + " seconds");

	}

	/**
	 * <ul>
	 * <li>Make sublists of size {@link Constants.TUPPLES_IN_BUFFER}</li>
	 * <li>Reads from file in bytes, so all the records needs to be of same
	 * size</li>
	 * </ul>
	 * 
	 * @param inputFile
	 * @param outputFile
	 * @throws IOException
	 */
	private static void sortSublist(File inputFile, File outputFile) throws IOException {
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(inputFile));
				FileChannel outChannel = new FileOutputStream(outputFile).getChannel()) {
			int lineSeparatorLength = System.lineSeparator().getBytes().length;
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					Constants.TUPPLES_IN_BUFFER * (Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength));

			byte[] receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];

			byte[][] tuples = new byte[Constants.TUPPLES_IN_BUFFER][];
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
				}

			}
		}
	}

	/**
	 * <p>
	 * Merge the sublists. Uses {@link Executors} framework to read from all the
	 * sublists parallely.
	 * </p>
	 * 
	 * @param inputFile
	 * @param outputFile
	 */
	private static void mergeSublist(File inputFile, File outputFile) {

		int noOfSublists = (int) Math.ceil((double) inputFile.length()
				/ ((Constants.TUPLE_SIZE_IN_BYTES + System.lineSeparator().getBytes().length)
						* Constants.TUPPLES_IN_BUFFER));
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
				executor.execute(new ReaderThread(startPoint[i], recordsToRead, inputFile, tuples[i]));
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
			int[] currentReadPointer = new int[noOfSublists];
			byte[][] sorted = new byte[recordsToRead][];
			boolean[] allRecordsFetched = new boolean[noOfSublists];
			int currentOutputPointer = 0;
			boolean allTuplesRead = false;
			outer: while (!allTuplesRead) {
				int j = -1;
				min = new byte[] { Byte.MAX_VALUE };
				minList = -1;
				for (int i = 0; i < noOfSublists; i++) {
					j = -1;
					if (allRecordsFetched[i]) {
						continue;
					} else if (recordsToRead - currentReadPointer[i] > 0 || currentReadPointer[i] < tuples[i].length) {
						j = currentReadPointer[i];
					} else if (!allRecordsFetched[i] && recordsFetched[i] >= Constants.TUPPLES_IN_BUFFER
							&& currentReadPointer[i] >= tuples[i].length) {
						allRecordsFetched[i] = true;
						count++;
					} else {
						j = 0;
						currentReadPointer[i] = 0;
						recordsToRead = (Constants.TUPPLES_IN_BUFFER - recordsFetched[i]) < recordsToRead
								? Constants.TUPPLES_IN_BUFFER - recordsFetched[i]
								: recordsToRead;
						tuples[i] = new byte[recordsToRead][];
						Runnable task = new ReaderThread(startPoint[i], recordsToRead, inputFile, tuples[i]);
						recordsFetched[i] += recordsToRead;
						startPoint[i] += recordsToRead;
						Thread t = new Thread(task);
						t.start();
						try {
							t.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}

					if (count == noOfSublists) {
						allTuplesRead = true;
						Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead));
						t.start();
						try {
							t.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						break outer;
					} else if (j != -1 && tuples[i] != null && tuples[i][j] != null
							&& bac.compare(tuples[i][j], min) > 0) {

						min = tuples[i][j];
						minList = i;
					}
				}
				if (minList == -1) {
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead));
					t.start();
					try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					break;
				}
				currentReadPointer[minList] += 1;
				if (currentOutputPointer >= recordsToRead) {
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead));
					t.start();
					try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					currentOutputPointer = 0;
					sorted = new byte[recordsToRead][];
					sorted[currentOutputPointer++] = min;
				} else {
					sorted[currentOutputPointer++] = min;
				}
			}

		}

	}

	/**
	 * Perform the bag difference
	 * 
	 * @param file1
	 * @param outputFile
	 * @throws IOException
	 */
	private static void bagDifference(File file1, File outputFile) throws IOException {
		try (ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(file1))) {

			int lineSeparatorLength = System.lineSeparator().getBytes().length;
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					Constants.TUPPLE_IN_BAG_DIFF * (Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength));

			byte[] receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];

			byte[][] tuples1 = new byte[Constants.TUPPLE_IN_BAG_DIFF][];
			byte[][] output = new byte[Constants.TUPPLE_IN_BAG_DIFF][];
			int counter = 0;
			int outputCounter = -1;
			byte[] record = null;
			int duplicates1 = 0;
			int diff = 0;
			int previousDuplicates = 0;
			int duplicates2 = 0;
			byte[] previous1 = null;
			outer: while (inChannel1.read(buffer) > 0) {
				buffer.flip();
				// read buffer
				while (buffer.hasRemaining()) {
					buffer.get(receive);
					tuples1[counter++] = receive;
					receive = null;
					receive = new byte[Constants.TUPLE_SIZE_IN_BYTES + lineSeparatorLength];
				}
				buffer.clear();

				// find duplicates in same file
				for (int i = 0; i < counter;) {

					if (outputCounter == Constants.TUPPLE_IN_BAG_DIFF - 1) {
						writeOutput(output, outputFile);
						output = null;
						output = new byte[Constants.TUPPLE_IN_BAG_DIFF][];
						outputCounter = -1;
					}

					record = tuples1[i];
					duplicates1 = 1;
					if (bac.compare(record, previous1) == 0) {
						previousDuplicates++;
					} else {

						if (previous1 != null) {
							duplicates2 = SearchDuplicates.findDuplicatesInFile(previous1);
							diff = previousDuplicates - duplicates2;
							output[++outputCounter] = formatOutput(previous1, diff);
						}
						previous1 = null;
						previousDuplicates = 0;
					}

					duplicates1 += findDuplicates(record, i + 1, tuples1);
					i += duplicates1;

					if (i >= counter) {
						counter = 0;
						if (previous1 != null) {
							duplicates1--;
							previousDuplicates += duplicates1;
						} else {
							previousDuplicates = duplicates1;
						}
						previous1 = record;
						record = null;
						continue outer;
					} else {
						if (previous1 != null)
							duplicates1--;
						previous1 = null;
						duplicates2 = SearchDuplicates.findDuplicatesInFile(record);

						if (outputCounter == Constants.TUPPLE_IN_BAG_DIFF - 1) {
							writeOutput(output, outputFile);
							output = null;
							output = new byte[Constants.TUPPLE_IN_BAG_DIFF][];
							outputCounter = -1;
						}

						diff = previousDuplicates + duplicates1 - duplicates2;
						output[++outputCounter] = formatOutput(record, diff);
					}

				}

			}

			if (record != null || previous1 != null) {
				byte[] toSearch = record == null ? previous1 : record;
				duplicates2 = SearchDuplicates.findDuplicatesInFile(toSearch);
				duplicates1 = record == null ? previousDuplicates : (previousDuplicates + duplicates1);
				diff = duplicates1 - duplicates2;
				output[++outputCounter] = formatOutput(toSearch, diff);
			}

			if (outputCounter >= 0) {
				writeOutput(output, outputFile);
				output = null;
			}

		}

	}

	/**
	 * Find similar byte arrays
	 * 
	 * @param recToBeSearched
	 * @param startPointer
	 * @param tuples
	 * @return
	 */
	private static int findDuplicates(byte[] recToBeSearched, int startPointer, byte[][] tuples) {
		int duplicates = 0;
		while (startPointer < tuples.length && tuples[startPointer] != null
				&& bac.compare(tuples[startPointer], recToBeSearched) == 0) {
			startPointer++;
			duplicates++;
		}
		return duplicates;
	}

	/**
	 * Formats the output buffer
	 * 
	 * @param record
	 * @param diff
	 * @return
	 */
	private static byte[] formatOutput(byte[] record, int diff) {
		byte[] b = (String.valueOf(" : " + (diff>0?diff:0)+" ")+System.lineSeparator()).getBytes();
		return ByteBuffer
				.allocate(record.length - 2 + b.length)
				.put(record, 0, record.length - 2).put(b).array();
		}

	/**
	 * writes the output to file
	 * 
	 * @param buffer
	 * @param file
	 */
	private static void writeOutput(byte[][] buffer, File file) {
		Thread t = new Thread(new WriterThread(buffer, file, Constants.TUPPLE_IN_BAG_DIFF, buffer[0].length));
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
