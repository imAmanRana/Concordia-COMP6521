/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.BUFFER_SIZE;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.MAIN_MEMORY_SIZE;
import static comp6521.Constants.TUPLES_IN_BLOCK;
import static comp6521.Constants.TUPPLES_IN_BUFFER;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author AmanRana
 *
 */
public class Main {

	public static int mainMemorySize;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		init();

		// read first file
		List<String> files;
		do {
			files = Utils.readFromFile(0, Constants.INPUT_FILE1_PATH);

			// sort the records
			Collections.sort(files);

			// write back to file
			Utils.write(files, true);

		} while (files != null && files.size() > 0);

		// read second file
		do {
			files = Utils.readFromFile(0, Constants.INPUT_FILE2_PATH);

			// sort the records
			Collections.sort(files);

			// write back to file
			Utils.write(files, true);

		} while (files != null && files.size() > 0);
	}

	private static void init() {
		// read the properties file
		Properties properties = new Properties();
		try (FileInputStream fin = new FileInputStream("application.properties")) {
			properties.load(fin);
			MAIN_MEMORY_SIZE = Integer.valueOf(properties.getProperty("MAIN_MEMORY_SIZE")) * 1024 * 1024;
			INPUT_FILE1_PATH = properties.getProperty("INPUT_FILE1_PATH");
			INPUT_FILE2_PATH = properties.getProperty("INPUT_FILE2_PATH");

			BUFFER_SIZE = MAIN_MEMORY_SIZE / BLOCK_SIZE;
			TUPPLES_IN_BUFFER = BUFFER_SIZE / TUPLES_IN_BLOCK;

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception");
			e.printStackTrace();
		}
	}

}
