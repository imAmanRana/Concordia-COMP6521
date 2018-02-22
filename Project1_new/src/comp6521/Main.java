/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCKS_IN_MEMORY;
import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.FINAL_OUTPUT;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE1_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE2_PATH;
import static comp6521.Constants.MAIN_MEMORY_SIZE;
import static comp6521.Constants.MEMORY_UTILIZATION;
import static comp6521.Constants.OUTPUT_FILE1_PATH;
import static comp6521.Constants.OUTPUT_FILE2_PATH;
import static comp6521.Constants.TUPLES_IN_BLOCK;
import static comp6521.Constants.TUPLE_SIZE_IN_BYTES;
import static comp6521.Constants.TUPPLES_IN_BUFFER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author AmanRana
 *
 */
public class Main {

	static List<String> list2 = new ArrayList<>();
	static int readPointList2 = 0;
	static int nextReadPointer = 1;
	static int startPointer2 = 1;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		// read the configuration file
		init();

		File inputFile1 = new File(INPUT_FILE1_PATH);
		File inputFile2 = new File(INPUT_FILE2_PATH);
		File outputFile1 = new File(OUTPUT_FILE1_PATH);
		File outputFile2 = new File(OUTPUT_FILE2_PATH);
		File intermediateOutputFile1 = new File(INTERMEDIATE_OUTPUT_FILE1_PATH);
		File intermediateOutputFile2 = new File(INTERMEDIATE_OUTPUT_FILE2_PATH);
		File finalOutput = new File(FINAL_OUTPUT);

		Utils.clearFile(outputFile1);
		Utils.clearFile(outputFile2);
		Utils.clearFile(intermediateOutputFile1);
		Utils.clearFile(intermediateOutputFile2);
		Utils.clearFile(finalOutput);

		// sublist sorting file 1
		long start = System.nanoTime();
		int noOfTuplesInR1 = sort(inputFile1, outputFile1);
		long end = System.nanoTime();
		System.out.println("Sublist sort for bag1 in : " + (end - start) / 1_000_000_000 + " seconds");

		// sublist sorting file 2
		start = System.nanoTime();
		int noOfTuplesInR2 = sort(inputFile2, outputFile2);
		end = System.nanoTime();
		System.out.println("Sublist sort for bag2 in : " + (end - start) / 1_000_000_000 + " seconds");

		// merging sublist in file 1
		start = System.nanoTime();
		File sortedFile1 = merge(noOfTuplesInR1, outputFile1, intermediateOutputFile1);
		end = System.nanoTime();
		System.out.println("Merged bag1 after : " + (end - start) / 1_000_000_000 + " seconds");

		// merging sublist in file 2
		start = System.nanoTime();
		File sortedFile2 = merge(noOfTuplesInR2, outputFile2, intermediateOutputFile2);
		end = System.nanoTime();
		System.out.println("Merged bag2 after : " + (end - start) / 1_000_000_000 + " seconds");

		// finding the bag difference, file1-file2
		start = System.nanoTime();
		bagDifference(sortedFile1, noOfTuplesInR1, sortedFile2, noOfTuplesInR2, finalOutput);
		end = System.nanoTime();
		System.out.println("Bag Difference after : " + (end - start) / 1_000_000_000 + " seconds");
	}

	private static void bagDifference(File sortedFile1, int noOfTuplesInR1, File sortedFile2, int noOfTuplesInR2,
			File finalOutput) throws IOException {
		int startPointer1 = 1;
		List<String> output = new ArrayList<>();
		int recordsToRead = (TUPPLES_IN_BUFFER / 3);
		List<String> list1;
		String record = null;
		String temp = null;
		boolean isFirstIteration = true;
		int duplicates1 = 0;
		int duplicates2 = 0;
		Iterator<String> listIterator;

		while (startPointer1 <= noOfTuplesInR1) {

			recordsToRead = (noOfTuplesInR1 - startPointer1) > recordsToRead ? recordsToRead
					: (noOfTuplesInR1 - startPointer1) + 1;
			list1=new ArrayList<>();
			Utils.readFromFile(list1, startPointer1, sortedFile1, recordsToRead);
			startPointer1 += list1.size();

			listIterator = list1.iterator();

			if (listIterator.hasNext()) {
				record = listIterator.next();

				if (temp != null && record.compareTo(temp) == 0) {
					duplicates1++;
				} else if (isFirstIteration) {
					duplicates1 = 1;
					isFirstIteration = false;
				} else {
					duplicates2 = performLinearSearch(temp, sortedFile2, noOfTuplesInR2);

					if (output.size() == recordsToRead) {
						Utils.write(output, finalOutput);
						output.clear();
					}
					if (duplicates1 > duplicates2) {
						output.add(temp + ": " + (duplicates1 - duplicates2));
					} else {
						output.add(temp + ": 0");
					}
					duplicates1 = 1;
				}

				while (listIterator.hasNext()) {
					temp = listIterator.next();
					if (record.compareTo(temp) == 0) {
						duplicates1++;
					} else {
						duplicates2 = performLinearSearch(record, sortedFile2, noOfTuplesInR2);

						if (output.size() == recordsToRead) {
							Utils.write(output, finalOutput);
							output.clear();
						}
						if (duplicates1 > duplicates2) {
							output.add(record + ": " + (duplicates1 - duplicates2));
						} else {
							output.add(record + ": 0");
						}
						record = temp;
						duplicates1 = 1;
					}

				}
			}

		}

		// compare the last record in the file
		duplicates2 = performLinearSearch(record, sortedFile2, noOfTuplesInR2);

		if (output.size() == recordsToRead) {
			Utils.write(output, finalOutput);
			output.clear();
		}
		if (duplicates1 > duplicates2) {
			output.add(record + ": " + (duplicates1 - duplicates2));
		} else {
			output.add(record + ": 0");
		}

		if (!output.isEmpty()) {
			Utils.write(output, finalOutput);
			output.clear();
		}

	}

	private static int performLinearSearch(String recordToSearch, File sortedFile, int noOfTuples) {
		int recordsToRead = (TUPPLES_IN_BUFFER / 3);
		Iterator<String> listIterator;
		String record = null;
		boolean isFirstIteration = true;
		int duplicates = 0;
		outer: while (startPointer2 <= noOfTuples) {

			if (readPointList2 >= list2.size()) {
				recordsToRead = (noOfTuples - startPointer2) > recordsToRead ? recordsToRead
						: (noOfTuples - startPointer2) + 1;
				list2 = Utils.readFromFile(new ArrayList<String>(),nextReadPointer, sortedFile, recordsToRead);
				nextReadPointer += list2.size();
				listIterator = list2.iterator();
				readPointList2 = 0;
			} else {
				listIterator = list2.listIterator(readPointList2);
			}

			while (listIterator.hasNext()) {
				record = listIterator.next();
				readPointList2++;
				startPointer2++;
				if (isFirstIteration && record.compareTo(recordToSearch) == 0) {
					duplicates = 1;
					isFirstIteration = false;
				} else if (record.compareTo(recordToSearch) == 0) {
					duplicates++;
				} else if (record.compareTo(recordToSearch) < 0) {
					// DO nothing
				} else {
					readPointList2--;
					startPointer2--;
					break outer;
				}

			}

		}
		return duplicates;

	}

	/**
	 * Reads the configuration file
	 */
	private static void init() {
		// read the properties file
		Properties properties = new Properties();
		try (InputStream inputStream = Main.class.getResourceAsStream("/application.properties")) {
			properties.load(inputStream);
			MAIN_MEMORY_SIZE = Integer.valueOf(properties.getProperty("MAIN_MEMORY_SIZE"));
			INPUT_FILE1_PATH = properties.getProperty("INPUT_FILE1_PATH");
			INPUT_FILE2_PATH = properties.getProperty("INPUT_FILE2_PATH");
			OUTPUT_FILE1_PATH = properties.getProperty("OUTPUT_FILE1_PATH");
			OUTPUT_FILE2_PATH = properties.getProperty("OUTPUT_FILE2_PATH");
			INTERMEDIATE_OUTPUT_FILE1_PATH = properties.getProperty("INTERMEDIATE_OUTPUT_FILE1_PATH");
			INTERMEDIATE_OUTPUT_FILE2_PATH = properties.getProperty("INTERMEDIATE_OUTPUT_FILE2_PATH");
			FINAL_OUTPUT = properties.getProperty("FINAL_OUTPUT");
			MEMORY_UTILIZATION = Integer.valueOf(properties.getProperty("MEMORY_UTILIZATION"));

			BLOCK_SIZE = Integer.valueOf(properties.getProperty("BLOCK_SIZE"));
			TUPLE_SIZE_IN_BYTES = Integer.valueOf(properties.getProperty("TUPLE_SIZE_IN_BYTES"));
			TUPLES_IN_BLOCK = Integer.valueOf(properties.getProperty("TUPLES_IN_BLOCK"));

			BLOCKS_IN_MEMORY = MAIN_MEMORY_SIZE / BLOCK_SIZE;
			TUPPLES_IN_BUFFER = BLOCKS_IN_MEMORY * TUPLES_IN_BLOCK;

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("IO Exception: " + e.getMessage());
		}
	}

	private static int sort(File inputFile, File outputFile) throws IOException {
		List<String> records;
		int noOfTuples = 1;
		int recordsTORead = TUPPLES_IN_BUFFER;
		
		do {
			records=new ArrayList<>();
			Utils.readFromFile(records,noOfTuples, inputFile, recordsTORead);

			/*
			 * Using Java's in-build sorting method(its much efficient)
			 */
			Collections.sort(records);
			// write back to file
			Utils.write(records, outputFile);
			noOfTuples += records.size();
		} while (records != null && !records.isEmpty());

		return noOfTuples-1;
	}

	private static File merge(int noOfTuples, File file, File intermediateFile) throws IOException {

		int noOfPasses = (int) Math.ceil(Utils.log2((int) Math.ceil((double) noOfTuples / TUPPLES_IN_BUFFER)));
		final int RECORDS_TO_READ = (TUPPLES_IN_BUFFER / 4);
		File readFile = null;
		File writeFile = file;
		List<String> mergedList = new ArrayList<>();
		// execute the passes
		for (int i = 1; i <= noOfPasses; i++) {
			int subListSize = TUPPLES_IN_BUFFER * (int) Math.pow(2, i - 1);

			// decide which file to use for reading and which to use for writing
			if (i % 2 == 1) {
				readFile = file;
				writeFile = intermediateFile;
			} else {
				readFile = intermediateFile;
				writeFile = file;
			}
			Utils.clearFile(writeFile);
			// no. of sublists for this pass
			int noOfSubList = (int) Math.ceil((double) noOfTuples / subListSize);

			// reading position of sublists
			int sublist1ReadPosition = 1;
			int sublist2ReadPosition = 1 + subListSize;
			int recordsRead1 = 0;
			int recordsRead2 = 0;
			List<String> sublist1=null;
			List<String> sublist2=null;

			// read 2 sublist at a time and merge them
			for (int j = 1; j <= noOfSubList; j += 2) {

				// do the merging
				int x = 0;
				int y = 0;
				sublist1 = new ArrayList<>();
				Utils.readFromFile(sublist1,sublist1ReadPosition, readFile, RECORDS_TO_READ);
				recordsRead1 += sublist1.size();
				sublist1ReadPosition += sublist1.size();

				sublist2 = new ArrayList<>();
				Utils.readFromFile(sublist2,sublist2ReadPosition, readFile, RECORDS_TO_READ);
				recordsRead2 += sublist2.size();
				sublist2ReadPosition += sublist2.size();
				boolean continueLoop = true;

				while (!sublist1.isEmpty() && !sublist2.isEmpty() && recordsRead1 <= subListSize
						&& recordsRead2 <= subListSize && continueLoop) {
					while (x < sublist1.size() && y < sublist2.size()) {

						if (sublist1.get(x).compareTo(sublist2.get(y)) <= 0) {
							mergedList.add(sublist1.get(x++));
						} else {
							mergedList.add(sublist2.get(y++));
						}

						if (mergedList.size() == TUPPLES_IN_BUFFER / 2) {
							Utils.write(mergedList, writeFile);
							mergedList.clear();
						}
					}

					if (x == sublist1.size()) {

						if (recordsRead1 == subListSize) {
							continueLoop = false;
						} else {
							sublist1 = new ArrayList<>();
							Utils.readFromFile(sublist1,sublist1ReadPosition, readFile, RECORDS_TO_READ);
							x = 0;
							recordsRead1 += sublist1.size();
							sublist1ReadPosition += RECORDS_TO_READ;
						}
					} else if (y == sublist2.size()) {

						if (recordsRead2 == subListSize) {
							continueLoop = false;
						} else {
							sublist2 = new ArrayList<>();
							Utils.readFromFile(sublist2,sublist2ReadPosition, readFile, RECORDS_TO_READ);
							y = 0;
							recordsRead2 += sublist2.size();
							sublist2ReadPosition += RECORDS_TO_READ;
						}
					}
				}

				// check which list is remaining
				if (recordsRead1 <= subListSize && x < sublist1.size()) {

					int availableMemorySize = TUPPLES_IN_BUFFER - mergedList.size() - sublist1.size() + x;

					if ((subListSize - recordsRead1) > availableMemorySize) {
						mergedList.addAll(sublist1.subList(x, sublist1.size()));
						mergedList.addAll(Utils.readFromFile(new ArrayList<String>(),sublist1ReadPosition, readFile, availableMemorySize));
						sublist1ReadPosition += availableMemorySize;
						recordsRead1 += availableMemorySize;
						Utils.write(mergedList, writeFile);
						mergedList.clear();

						for (int size = (subListSize - recordsRead1); size > 0; size -= TUPPLES_IN_BUFFER) {
							if (size >= TUPPLES_IN_BUFFER) {
								sublist1 = Utils.readFromFile(new ArrayList<String>(),sublist1ReadPosition, readFile, TUPPLES_IN_BUFFER);
								sublist1ReadPosition += TUPPLES_IN_BUFFER;
								recordsRead1 += sublist1.size();
							} else {
								sublist1 = new ArrayList<>();
								Utils.readFromFile(sublist1,sublist1ReadPosition, readFile, size);
								sublist1ReadPosition += size;
								recordsRead1 += sublist1.size();
							}
							Utils.write(sublist1, writeFile);
						}

					} else {

						mergedList.addAll(sublist1.subList(x, sublist1.size()));
						sublist1 = new ArrayList<>();
						Utils.readFromFile(sublist1,sublist1ReadPosition, readFile, (subListSize - recordsRead1));
						mergedList.addAll(sublist1);
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						recordsRead1 += sublist1.size();
						sublist1ReadPosition += (subListSize - recordsRead1);

					}
				} else if (recordsRead2 <= subListSize && y < sublist2.size()) {
					int availableMemorySize = TUPPLES_IN_BUFFER - mergedList.size() - sublist2.size() + y;

					if ((subListSize - recordsRead2) > availableMemorySize) {
						mergedList.addAll(sublist2.subList(y, sublist2.size()));
						mergedList.addAll(Utils.readFromFile(new ArrayList<String>(),sublist2ReadPosition, readFile, availableMemorySize));
						sublist2ReadPosition += availableMemorySize;
						recordsRead2 += availableMemorySize;
						Utils.write(mergedList, writeFile);
						mergedList.clear();

						for (int size = (subListSize - recordsRead2); size > 0; size -= TUPPLES_IN_BUFFER) {
							if (size >= TUPPLES_IN_BUFFER) {
								sublist2 = new ArrayList<>();
								Utils.readFromFile(sublist2,sublist2ReadPosition, readFile, TUPPLES_IN_BUFFER);
								sublist2ReadPosition += TUPPLES_IN_BUFFER;
								recordsRead2 += sublist2.size();
							} else {
								sublist2 = new ArrayList<>();
								Utils.readFromFile(sublist2,sublist2ReadPosition, readFile, size);
								sublist2ReadPosition += size;
								recordsRead2 += sublist2.size();
							}
							Utils.write(sublist2, writeFile);
						}

					} else {
						mergedList.addAll(sublist2.subList(y, sublist2.size()));
						sublist2 = new ArrayList<>();
						Utils.readFromFile(sublist2,sublist2ReadPosition, readFile, (subListSize - recordsRead2));
						mergedList.addAll(sublist2);
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						recordsRead2 += sublist2.size();
						sublist2ReadPosition += (subListSize - recordsRead2);
					}
				}

				sublist1ReadPosition += subListSize;
				sublist2ReadPosition += subListSize;
				recordsRead1 = 0;
				recordsRead2 = 0;
				mergedList.clear();

			}

		}
		return writeFile;
	}

}
