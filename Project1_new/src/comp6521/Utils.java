package comp6521;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Karan
 * @version 1.0 Handles the read and write operations.
 */
public class Utils {

	private Utils() {

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
	 * Writes the sorted sublists to the output bag.
	 * 
	 * @param sublist
	 *            contains the sorted tuples
	 * @param file
	 *            file to write the sublist
	 * @throws IOException
	 */
	public static void write(List<String> sublist, File file) throws IOException {

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file, true))) {
			for (String tuple : sublist) {
				bw.write(tuple);
				bw.newLine();
			}
		} catch (Exception e) {
			System.out.println("Exception is writing to file " + e.getMessage());
		}
	}

	/**
	 * Code reference:
	 * {@link https://bitsofinfo.wordpress.com/2009/04/15/how-to-read-a-specific-line-from-a-very-large-file-in-java/}
	 * @param records 
	 * 
	 * @param startPoint
	 *            line where to start reading from file
	 * @param file
	 *            file to read
	 * @param recordsToRead
	 *            no. of records to read
	 * @return list of String(lines read from file)
	 */
	public static List<String> readFromFile(List<String> records, int startPoint, File file, int recordsToRead) {

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			long startAtByte = getSeekPosition(startPoint, file);
			br.skip(startAtByte);
			
			while (recordsToRead > 0 && (line = br.readLine()) != null) {
				records.add(line);
				recordsToRead--;
				line=null;
			}
		} catch (Exception e) {
			System.out.println("Exception while reading from file " + e.getMessage());
		}

		return records;
	}

	/**
	 * Seeks to a specific position in file
	 * 
	 * @param startPoint
	 *            point where to seek
	 * @param file
	 *            input file name
	 * @return no of bytes to skip.
	 */
	private static long getSeekPosition(int startPoint, File file) {
		long startAtByte = 0;
		long seekToByte = (startPoint == 1 ? 0
				: ((startPoint - 1) * (Constants.TUPLE_SIZE_IN_BYTES + System.lineSeparator().getBytes().length)));

		try (RandomAccessFile rand = new RandomAccessFile(file, "r")) {
			rand.seek(seekToByte);
			startAtByte = rand.getFilePointer();
		} catch (Exception e) {
			System.out.println("Exception while seeking in file " + e.getMessage());
		}
		return startAtByte;
	}

	public static List<String> readLinearlyFromFile(int startPoint, File file, int recordsToRead) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
			List<String> tuples = new ArrayList<>();
			String line;
			int records = 0;
			while (recordsToRead > 0 && (line = br.readLine()) != null) {
				records++;
				if (records < startPoint)
					continue;

				tuples.add(line);
				recordsToRead--;
			}
			return tuples;
		}
	}

	public static void memoryProfiler() {
		/* Total number of processors or cores available to the JVM */
		System.out.println("Available processors (cores): " + Runtime.getRuntime().availableProcessors());

		/* Total amount of free memory available to the JVM */
		System.out.println("Free memory (bytes): " + Runtime.getRuntime().freeMemory());

		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();
		/* Maximum amount of memory the JVM will attempt to use */
		System.out.println("Maximum memory (bytes): " + (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

		/* Total memory currently in use by the JVM */
		System.out.println("Total memory (bytes): " + Runtime.getRuntime().totalMemory());
	}

	/**
	 * Perform log2 calculations
	 * 
	 * @param n
	 *            number
	 * @return log2 of number
	 */
	public static double log2(int n) {
		return (Math.log(n) / Math.log(2));
	}
}
