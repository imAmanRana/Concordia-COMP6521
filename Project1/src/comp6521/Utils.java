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
	private static final String BAG1_OUTPUT_FILE_PATH = "src\\resources\\outputBag1.txt";
	private static final String BAG2_OUTPUT_FILE_PATH = "src\\resources\\outputBag2.txt";

	private static BufferedReader br;
	private static List<String> tuples = new ArrayList<>();

	/**
	 * Writes the sorted sublists to the output bag.
	 * 
	 * @param sublist
	 *            contains the sorted tuples
	 * @param isBag1
	 *            check whether the sublist belongs to bag 1 or bag 2.
	 * @throws IOException
	 */
	public static void write(List<String> sublist, boolean isBag1) throws IOException {

		try {

			if (isBag1) {

				File file = new File(BAG1_OUTPUT_FILE_PATH);

				if (!file.exists()) {
					System.out.println("We had to make a new file.");
					file.createNewFile();
				}

				bufferedWriter = new BufferedWriter(new FileWriter(file, true));
				for (String tuple : sublist) {

					bufferedWriter.write(tuple);
				}

			} else {

				File file = new File(BAG2_OUTPUT_FILE_PATH);

				if (!file.exists()) {
					System.out.println("We had to make a new file.");
					file.createNewFile();
				}

				bufferedWriter = new BufferedWriter(new FileWriter(file, true));
				for (String tuple : sublist) {

					bufferedWriter.write(tuple);
				}

			}
		} catch (Exception e) {

			e.printStackTrace();
		} finally {

			bufferedWriter.close();
		}

	}

	public static List<String> readFromFile(int startPoint, String Filename) {

		int endpoint = startPoint;
		long startAtByte = 0;
		long seekToByte = (startPoint == 1 ? 0 : ((startPoint - 1) * (Constants.TUPLE_SIZE_IN_BYTES + 1)));

		try {
			RandomAccessFile rand = new RandomAccessFile(Filename, "r");
			rand.seek(seekToByte);
			startAtByte = rand.getFilePointer();

			rand.close();

			br = new BufferedReader(new InputStreamReader(new FileInputStream(Filename)));
			br.skip(startAtByte);
			while (endpoint - startPoint < 40) {
				tuples.add(br.readLine());
				endpoint++;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return tuples;
	}

}
