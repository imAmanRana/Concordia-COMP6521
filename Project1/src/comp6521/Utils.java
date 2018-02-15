package comp6521;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Karan
 * @version 1.0 Handles the read and write operations for the schemas.
 */
public class Utils {

	private static BufferedWriter bufferedWriter;

	private static BufferedReader br;

	/**
	 * Writes the sorted sublists to the output bag.
	 * 
	 * @param sublist
	 *            contains the sorted tuples
	 * @param isBag1
	 *            check whether the sublist belongs to bag 1 or bag 2.
	 * @throws IOException
	 */
	public static void write(List<String> sublist, File file) throws IOException {

		try {
			if (!file.exists()) {
				System.out.println("We had to make a new file.");
				file.createNewFile();
			}

			bufferedWriter = new BufferedWriter(new FileWriter(file, true));
			for (String tuple : sublist) {

				bufferedWriter.write(tuple);
				bufferedWriter.newLine();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			bufferedWriter.close();
		}

	}

	/**
	 * Code reference:
	 * {@link https://bitsofinfo.wordpress.com/2009/04/15/how-to-read-a-specific-line-from-a-very-large-file-in-java/}
	 * 
	 * @param startPoint
	 * @param Filename
	 * @return
	 */
	public static List<String> readFromFile(int startPoint, File Filename,int recordsToRead) {
		long startAtByte = 0;
		long seekToByte = (startPoint == 1 ? 0 : ((startPoint - 1) * (Constants.TUPLE_SIZE_IN_BYTES + System.lineSeparator().getBytes().length)));
		List<String> tuples = new ArrayList<>();
		String line;
		try {
			RandomAccessFile rand = new RandomAccessFile(Filename, "r");
			rand.seek(seekToByte);
			startAtByte = rand.getFilePointer();

			rand.close();

			br = new BufferedReader(new InputStreamReader(new FileInputStream(Filename)));
			br.skip(startAtByte);
			
			while(recordsToRead>0 && (line=br.readLine())!=null ) {
				tuples.add(line);
				recordsToRead--;
			}
			
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return tuples;
	}

	public static double log2(int n)
	{
	    return (Math.log(n) / Math.log(2));
	}
}
