/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCKS_IN_MEMORY;
import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.MAIN_MEMORY_SIZE;
import static comp6521.Constants.OUTPUT_FILE1_PATH;
import static comp6521.Constants.OUTPUT_FILE2_PATH;
import static comp6521.Constants.TUPLES_IN_BLOCK;
import static comp6521.Constants.TUPPLES_IN_BUFFER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author AmanRana
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		init();

		// read first file
		List<String> records;
		int readLine=1;
		do {
			records = Utils.readFromFile(readLine, new File(Main.class.getResource(Constants.INPUT_FILE1_PATH).getFile()));

			//System.out.println(records);
			
			// sort the records
			Collections.sort(records);

			// write back to file
			Utils.write(records, true);
			readLine+=TUPPLES_IN_BUFFER;

		} while (records != null && !records.isEmpty());

		// read second file
		readLine=1;
		do {
			records = Utils.readFromFile(readLine, new File(Main.class.getResource(Constants.INPUT_FILE2_PATH).getFile()));

			// sort the records
			Collections.sort(records);

			// write back to file
			Utils.write(records, false);
			readLine+=TUPPLES_IN_BUFFER;

		} while (records != null && !records.isEmpty());
	}

	private static void init() {
		// read the properties file
		Properties properties = new Properties();
		try(InputStream inputStream = Main.class.getResourceAsStream("/application.properties")) {
			properties.load(inputStream);
			MAIN_MEMORY_SIZE = Integer.valueOf(properties.getProperty("MAIN_MEMORY_SIZE"));
			INPUT_FILE1_PATH = properties.getProperty("INPUT_FILE1_PATH");
			INPUT_FILE2_PATH = properties.getProperty("INPUT_FILE2_PATH");
			OUTPUT_FILE1_PATH = properties.getProperty("OUTPUT_FILE1_PATH");
			OUTPUT_FILE2_PATH = properties.getProperty("OUTPUT_FILE2_PATH");
			BLOCK_SIZE = Integer.valueOf(properties.getProperty("BLOCK_SIZE"));

			BLOCKS_IN_MEMORY = MAIN_MEMORY_SIZE / BLOCK_SIZE;
			TUPPLES_IN_BUFFER = BLOCKS_IN_MEMORY*TUPLES_IN_BLOCK;

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception");
			e.printStackTrace();
		}
	}

}
