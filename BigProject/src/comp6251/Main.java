/**
 * 
 */
package comp6251;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author Amandeep Singh
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
				clearFile(new File(Constants.NESTEDJOIN_OUTPUT_FILE));
				long start = System.nanoTime();
				//performNestedJoin(new File(Constants.INPUT_FILE1), new File(Constants.INPUT_FILE2),
				//		new File(Constants.NESTEDJOIN_OUTPUT_FILE));
				optimized(new File(Constants.INPUT_FILE1), new File(Constants.INPUT_FILE2),new File(Constants.NESTEDJOIN_OUTPUT_FILE));
				long end = System.nanoTime();
				System.out.println("Sublist Sort : " + (end - start) / 1_000_000_000 + " seconds");
				break;
			case "2":
				clearFile(new File(Constants.INTERMEDIATE_T1));
				clearFile(new File(Constants.SORTED_T1));
				sortAndMerge(new File(Constants.INPUT_FILE1),new File(Constants.SORTED_T1),new File(Constants.INTERMEDIATE_T1),Constants.T1_TUPPLES_IN_BUFFER_FOR_SORT,Constants.TUPPLE_SIZE_T1);
				break;
			case "3":
				clearFile(new File(Constants.INTERMEDIATE_T2));
				clearFile(new File(Constants.SORTED_T2));
				sortAndMerge(new File(Constants.INPUT_FILE2),new File(Constants.SORTED_T2),new File(Constants.INTERMEDIATE_T2),Constants.T2_TUPPLES_IN_BUFFER_FOR_SORT,Constants.TUPPLE_SIZE_T2);
				break;
			case "4":
				clearFile(new File(Constants.GRADES_FILE));
				clearFile(new File(Constants.SORTEDJOIN_OUTPUT_FILE));
				start = System.nanoTime();
				performSortedJoin(new File(Constants.SORTED_T1),new File(Constants.SORTED_T2),new File(Constants.SORTEDJOIN_OUTPUT_FILE),new File(Constants.GRADES_FILE));
				end = System.nanoTime();
				System.out.println("Sorted Join : " + (end - start) / 1_000_000_000 + " seconds");
				break;
			default:
				printHelp();
				break;
			}
		}
	}

	
	private static void performSortedJoin(File sortedT1, File sortedT2, File outputFile,File gradesFile) throws IOException {
		
		
		try (ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(sortedT1));
				ReadableByteChannel inChannel2 = Channels.newChannel(new FileInputStream(sortedT2));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(outputFile));
				WritableByteChannel gradesChannel = Channels.newChannel(new FileOutputStream(gradesFile))) {

			ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.T1_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN
					* (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.T2_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN
					* (Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH));

			ByteBuffer outputBuffer = ByteBuffer.allocateDirect(Constants.T3_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN
					* (Constants.TUPPLE_SIZE_T3 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer gradesBuffer = ByteBuffer.allocateDirect(Constants.GRADE_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN
					* (Constants.TUPPLE_SIZE_GRADES + Constants.LINE_SEPARATOR_LENGTH));

			byte[] record1;
			byte[] record2;
			int noOfRecordsInFile1 = findRecordsInFile(sortedT1,
					(Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
			int startPointer1 = 0;

			inChannel2.read(buffer2);
			buffer2.flip();

			record1 = new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];
			record2 = new byte[Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH];
			
			int oldStudentId=0;
			int newStudentId;
			int denominator=0;
			float numerator = 0;
			buffer2.get(record2);
			
			while (startPointer1 < noOfRecordsInFile1) {
				
				buffer1.clear();
				inChannel1.read(buffer1);
				
				startPointer1 += (buffer1.position()
						/ (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
				
				buffer1.flip();
				oldStudentId = getIntegerData(record1,0,8);
				
				
				while (buffer1.hasRemaining() || bac.compare(record1, record2)==0) {					
					int value = bac.compare(record1, record2);
					
					if (value == 0) // ids are equal
					{
						newStudentId = getIntegerData(record2,0,8);
						if(oldStudentId==newStudentId) {
							int credit = getIntegerData(record2, 21, 2);
							String grade = getStringData(record2, 23, 4);
							numerator += credit*gradeToMarks(grade);
							denominator+= credit;							
						}
						
						if (outputBuffer.position() < outputBuffer.capacity()) {
							outputBuffer.put(combine(record1, record2));
						} else {
							outputBuffer.flip();
							outChannel.write(outputBuffer);
							outputBuffer.clear();
							outputBuffer.put(combine(record1, record2));			
						}
						
						if (buffer2.hasRemaining()) {
							buffer2.get(record2);
						} 
						else {								// id in t1 is bigger
							buffer2.clear();
							inChannel2.read(buffer2);
							buffer2.flip();
						}
					
					} else if (value > 0) // id in t2 is bigger
					{
						if (buffer1.hasRemaining()) {
							buffer1.get(record1); // id in t1 is bigger
							if(denominator>0) {
								if(gradesBuffer.position()==gradesBuffer.capacity())
								{
									gradesBuffer.flip();
									gradesChannel.write(gradesBuffer);
									gradesBuffer.clear();
								}
								gradesBuffer.put(convertToBuffer(oldStudentId,String.format("%.2f", numerator/denominator).getBytes()));
							}
							oldStudentId = getIntegerData(record1,0,8);
							numerator=0;
							denominator=0;
						}
						else {
							buffer1.clear();
							inChannel1.read(buffer1);
							startPointer1 += (buffer1.position()
									/ (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
							buffer1.flip();

						}
					}
					else {						// id in t1 is bigger
						if (buffer2.hasRemaining())
							buffer2.get(record2); 
						else {
							buffer2.clear();
							inChannel2.read(buffer2);
							buffer2.flip();
						}
					}
				}
			}
			if (outputBuffer.position() > 0) {
				outputBuffer.flip();
				outChannel.write(outputBuffer);
				outputBuffer = null;
			}
			if(denominator>0) {
				gradesBuffer.put(convertToBuffer(oldStudentId,String.format("%.2f", numerator/denominator).getBytes()));
			}
			if (gradesBuffer.position() > 0) {
				gradesBuffer.flip();
				gradesChannel.write(gradesBuffer);
				gradesBuffer = null;
			}
			
		}
	}


	private static ByteBuffer convertToBuffer(int studentId, byte[] gpa) {
		ByteBuffer buffer = ByteBuffer.allocate(Constants.STUDENT_ID_LENGTH + 5 + Constants.LINE_SEPARATOR_LENGTH)
				.put((studentId+" ").getBytes())
				.put(gpa)
				.put(System.lineSeparator().getBytes());
		buffer.flip();
		return buffer;
	}


	private static float gradeToMarks(String grade) {
		float marks = 0;
		switch (grade) {
		case "A+":
			marks = 4.3f;
			break;
		case "A":
			marks = 4.0f;
			break;
		case "A-":
			marks = 3.7f;
			break;
		case "B+":
			marks = 3.3f;
			break;
		case "B":
			marks = 0.0f;
			break;
		case "B-":
			marks = 2.7f;
			break;
		case "C+":
			marks = 2.3f;
			break;
		case "C":
			marks = 2.0f;
			break;
		case "C-":
			marks = 1.7f;
			break;
		case "D+":
			marks = 1.3f;
			break;
		case "D":
			marks = 1.0f;
			break;
		case "D-":
			marks = 0.7f;
			break;
		default:
			marks = 0.0f;
			break;
		}
		return marks;
	}


	private static String getStringData(byte[] record, int start, int end) {
		String s = new String(record, start, end);
		return s.trim();
	}

	private static int getIntegerData(byte[] record, int start, int end) {
		String s = new String(record, start, end).trim();
		if (s.isEmpty()) {
			return 0;
		} else {
			return Integer.parseInt(s);
		}
	}

	private static int findRecordsInFile(File fileName, int tupleSize) {
		return (int) ((double) fileName.length() / tupleSize);
	}


	/**
	 * Performs sublist sorting and then merging of the input file
	 * 
	 * @param inputFile
	 * @param outputFile
	 * @param intermediateFile
	 * @throws IOException
	 */
	private static void sortAndMerge(File inputFile, File outputFile, File intermediateFile,int tuplesInBuffer,int tupleSize) throws IOException {

		// sublist sorting file 1
		long start = System.nanoTime();
		sortSublist(inputFile, intermediateFile,tuplesInBuffer,tupleSize);
		long end = System.nanoTime();
		System.out.println("Sublist Sort : " + (end - start) / 1_000_000_000 + " seconds");

		start = System.nanoTime();
		mergeSublist(intermediateFile, outputFile,tuplesInBuffer,tupleSize);
		end = System.nanoTime();
		System.out.println("Merging : " + (end - start) / 1_000_000_000 + " seconds");

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
	private static void mergeSublist(File inputFile, File outputFile,int tuplesInBuffer,int tupleSize ) {

		int noOfSublists = (int) Math.ceil((double) inputFile.length()
				/ ((tupleSize + Constants.LINE_SEPARATOR_LENGTH)
						* tuplesInBuffer));
		
		System.out.println("input file length : "+inputFile.length()+"\nno of sublists: "+noOfSublists);
		System.out.println("tuple size : "+tupleSize);
		System.out.println("tuplesInBuffer : "+tuplesInBuffer);
		
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
			ExecutorService executor = Executors.newFixedThreadPool(noOfSublists);
			for (int i = 0; i < noOfSublists; i++) {
				tuples[i] = new byte[recordsToRead][];
				executor.execute(new ReaderThread(startPoint[i], recordsToRead, inputFile, tuples[i],tupleSize));
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
						Runnable task = new ReaderThread(startPoint[i], recordsToRead, inputFile, tuples[i],tupleSize);
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
						Thread t = new Thread(new WriterThread(sorted, outputFile));
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
					Thread t = new Thread(new WriterThread(sorted, outputFile));
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
					Thread t = new Thread(new WriterThread(sorted, outputFile));
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
	private static void sortSublist(File inputFile, File outputFile,int tuplesInBuffer, int tupleSize) throws IOException {
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(inputFile));
				FileChannel outChannel = new FileOutputStream(outputFile).getChannel()) {
			ByteBuffer buffer = ByteBuffer.allocateDirect(
					tuplesInBuffer * (tupleSize+ Constants.LINE_SEPARATOR_LENGTH));

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
					receive = new byte[tupleSize+ Constants.LINE_SEPARATOR_LENGTH];
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
	
	private static void optimized(File inputFile1, File inputFile2, File outputFile) throws IOException {
		try (FileInputStream in1 = new FileInputStream(inputFile1);
				FileOutputStream out = new FileOutputStream(outputFile);) {

			byte[] receive1 = new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];
			byte[] receive2;
			while (in1.read(receive1) > 0) {
				try (FileInputStream in2 = new FileInputStream(inputFile2)) {
					 receive2 = new byte[Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH];
					while (in2.read(receive2) > 0) {
						if (bac.compare(receive1, receive2) == 0) {
							System.out.println("equal");
							out.write(combine(receive1, receive2));
						}
						receive2  = new byte[Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH];
						
					}

				}
				receive1=new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];;
			}

		}
	}
	
	
	
	private static void performNestedJoin(File inputFile1, File inputFile2, File outputFile) throws IOException {

		try (ReadableByteChannel inChannel1 = Channels.newChannel(new FileInputStream(inputFile1));
				InputStream ins = new FileInputStream(inputFile2);
				FileChannel outputChannel = new FileOutputStream(outputFile).getChannel();) {
			ByteBuffer buffer1 = ByteBuffer.allocateDirect(
					Constants.T1_TUPPLES_IN_BUFFER_FOR_NESTED * (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer buffer2 = ByteBuffer.allocateDirect(
					Constants.T2_TUPPLES_IN_BUFFER_FOR_NESTED * (Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer outputBuffer = ByteBuffer.allocateDirect(
					Constants.T3_TUPPLES_IN_BUFFER_FOR_NESTED * (Constants.TUPPLE_SIZE_T3 + Constants.LINE_SEPARATOR_LENGTH));

			byte[] receive1 = null;
			byte[] receive2 = null;
			int counter = 0;
			byte[][] tuplesT1 = null;
			byte[][] tuplesT2 = null;
			int readt2=0;
			int readt1=0;
			outer: while (inChannel1.read(buffer1) > 0) {
				buffer1.flip();
				counter = 0;
				tuplesT1 = new byte[Constants.T1_TUPPLES_IN_BUFFER_FOR_NESTED][];
				while (buffer1.hasRemaining()) {
					receive1 = new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];
					buffer1.get(receive1);
					tuplesT1[counter++] = receive1;
				}
				receive1 = null;
				buffer1 = null;

				endLoopT1: try (ReadableByteChannel inChannel2 = Channels.newChannel(ins)) {
					while (inChannel2.read(buffer2) > 0) {
						buffer2.flip();
						counter = 0;
						tuplesT2 = new byte[Constants.T2_TUPPLES_IN_BUFFER_FOR_NESTED][];
						while (buffer2.hasRemaining()) {
							receive2=new byte[Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH];
							buffer2.get(receive2);
							tuplesT2[counter++] = receive2;
						}
						receive2 = null;
						buffer2 = null;
						// compare both tuples
						for (int i = 0; i < tuplesT1.length; i++) {
							for (int j = 0; j < tuplesT2.length; j++) {

								if (tuplesT1[i] == null)
									break outer;
								else if (tuplesT2[j] == null) {
									break endLoopT1;
								} else {
									if (bac.compare(tuplesT1[i], tuplesT2[j]) == 0) {
										if (outputBuffer.position() == outputBuffer.capacity()) {
											outputBuffer.flip();
											outputChannel.write(outputBuffer);
											outputBuffer.clear();
										}
										outputBuffer.put(combine(tuplesT1[i], tuplesT2[j]));
									}
								}
							}
						}

						tuplesT2 = null;
						buffer2 = ByteBuffer.allocateDirect(Constants.T2_TUPPLES_IN_BUFFER_FOR_NESTED
								* (Constants.TUPPLE_SIZE_T2 + Constants.LINE_SEPARATOR_LENGTH));
						System.out.println("reading b2 again: "+(readt2++));

					}
				}
				tuplesT1 = null;
				buffer1 = ByteBuffer.allocateDirect(
						Constants.T1_TUPPLES_IN_BUFFER_FOR_NESTED * (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
				System.out.println("read b1 again: "+(readt1++));
			}

			if (outputBuffer.position() > 0) {
				outputBuffer.flip();
				outputChannel.write(outputBuffer);
				outputBuffer.clear();
			}
		}

	}

	private static byte[] combine(byte[] tupleT1, byte[] tupleT2) {
		byte[] result = new byte[tupleT1.length + tupleT2.length - Constants.STUDENT_ID_LENGTH
				- Constants.LINE_SEPARATOR_LENGTH];

		for (int i = 0; i < tupleT1.length - Constants.LINE_SEPARATOR_LENGTH; i++) {
			result[i] = tupleT1[i];
		}
		for (int j = Constants.STUDENT_ID_LENGTH; j < tupleT2.length; j++) {
			result[tupleT1.length + j - Constants.STUDENT_ID_LENGTH - Constants.LINE_SEPARATOR_LENGTH] = tupleT2[j];
		}

		return result;

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

	public static void printHelp() {
		System.out.println("Please provide a valid command line input");
		System.out.println("1 - Nested Join on files :" + Constants.INPUT_FILE1 + " & " + Constants.INPUT_FILE2);
		System.out.println("2 - Sorting and Merging for :" + Constants.INPUT_FILE1);
		System.out.println("3 - Sorting and Merging for :" + Constants.INPUT_FILE2);
		System.out.println("4 - Sort Based Join for  :" + Constants.INPUT_FILE1 + " & " + Constants.INPUT_FILE2);
	}

}
