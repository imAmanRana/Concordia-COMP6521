package comp6521.project2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import comp6521.project2.utils.ByteArrayComparator;
import comp6521.project2.utils.Constants;
import comp6521.project2.utils.ReaderThread;
import comp6521.project2.utils.WriterThread;

public class Sort {

	static ByteArrayComparator bac = new ByteArrayComparator();

	void sortAndMerge(File inputFile, File outputFile, File intermediateFile, int tupleSize, int tuplesInBuffer)
			throws IOException {

		// sublist sorting file 1
		long start = System.nanoTime();
		sortSublist(inputFile, intermediateFile, tupleSize, tuplesInBuffer);
		long end = System.nanoTime();
		System.out.println("Sublist Sort : " + (end - start) / 1_000_000_000 + " seconds");

		start = System.nanoTime();
		mergeSublist(intermediateFile, outputFile, tupleSize, tuplesInBuffer);
		end = System.nanoTime();
		System.out.println("Merging : " + (end - start) / 1_000_000_000 + " seconds");

	}

	void sortSublist(File inputFile, File outputFile, int tupleSize, int tuplesInBuffer) throws IOException {
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(inputFile));
				FileChannel outChannel = new FileOutputStream(outputFile).getChannel()) {

			ByteBuffer buffer = ByteBuffer
					.allocateDirect(tuplesInBuffer * (tupleSize + Constants.LINE_SEPARATOR_LENGTH));

			byte[] receive = new byte[tupleSize + Constants.LINE_SEPARATOR_LENGTH];

			byte[][] tuples = new byte[tuplesInBuffer][];
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

				if (tuples.length == tuplesInBuffer) {
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
					tuples = new byte[tuplesInBuffer][];
				}

			}
		}
	}

	void mergeSublist(File inputFile, File outputFile, int tupleSize, int tuplesInBuffer) {

		int noOfSublists = (int) Math.ceil((double) inputFile.length()
				/ ((tupleSize + System.lineSeparator().getBytes().length) * tuplesInBuffer));
		int[] recordsFetched = new int[noOfSublists];
		int[] startPoint = new int[noOfSublists];
		int recordsToRead = (int) ((double) tuplesInBuffer / (noOfSublists * 2));
		recordsToRead = recordsToRead == 0 ? 1 : recordsToRead;

		for (int i = 0; i < noOfSublists; i++) {
			startPoint[i] = i * tuplesInBuffer;
		}

		byte[][][] tuples = new byte[noOfSublists][][];
		// read parallely from all sublists
		{
			System.out.println(noOfSublists);
			ExecutorService executor = Executors.newFixedThreadPool(noOfSublists);
			for (int i = 0; i < noOfSublists; i++) {
				tuples[i] = new byte[recordsToRead][];
				executor.execute(new ReaderThread(startPoint[i], recordsToRead, inputFile, tupleSize, tuples[i]));
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
					} else if (!allRecordsFetched[i] && recordsFetched[i] >= tuplesInBuffer
							&& currentReadPointer[i] >= tuples[i].length) {
						allRecordsFetched[i] = true;
						count++;
					} else {
						j = 0;
						currentReadPointer[i] = 0;
						recordsToRead = (tuplesInBuffer - recordsFetched[i]) < recordsToRead
								? tuplesInBuffer - recordsFetched[i]
								: recordsToRead;
						tuples[i] = new byte[recordsToRead][];
						Runnable task = new ReaderThread(startPoint[i], recordsToRead, inputFile, tupleSize, tuples[i]);
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
						Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead, tupleSize));
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
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead, tupleSize));
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
					Thread t = new Thread(new WriterThread(sorted, outputFile, recordsToRead, tupleSize));
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
}
