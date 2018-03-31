package join;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import join.ByteArrayComparator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import join.Constants;

public class Main {
	
	static NestedJoin nesjoin = new NestedJoin();
	static Utils utils = new Utils();
	static Sort sort = new Sort();
	static SortedJoin sortedjoin = new SortedJoin();
	
	
	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			printHelp();
		} else {
			switch (args[0]) {

			case "1":
				long start = System.nanoTime();
				nesjoin.Join(Constants.INPUT_FILE1,Constants.INPUT_FILE2,Constants.NESTEDJOIN_OUTPUT_FILE);
				long end = System.nanoTime();
				System.out.println("Nested Join: " + (end - start) / 1_000_000_000 + " seconds");
				break;
				
			case "2":
				utils.clearFile(new File(Constants.OUTPUT_FILE1));
				utils.clearFile(new File(Constants.MERGED_OUTPUT1));
				// do sorting and merging for bag1
				System.out.println("T1 Sorting");
				sort.sortAndMerge(new File(Constants.INPUT_FILE1), new File(Constants.OUTPUT_FILE1),
						new File(Constants.MERGED_OUTPUT1),Constants.TUPLE_SIZE_IN_BYTES_T1,Constants.TUPPLES_IN_BUFFER_T1_SORT);
				break;
				
			case "3":
				utils.clearFile(new File(Constants.OUTPUT_FILE2));
				utils.clearFile(new File(Constants.MERGED_OUTPUT2));
				// do sorting and merging for bag2
				System.out.println("T2 Sorting");
				sort.sortAndMerge(new File(Constants.INPUT_FILE2), new File(Constants.OUTPUT_FILE2),
						new File(Constants.MERGED_OUTPUT2),Constants.TUPLE_SIZE_IN_BYTES_T2,Constants.TUPPLES_IN_BUFFER_T2_SORT);
				break;
			case "4":
				start = System.nanoTime();
				sortedjoin.sortJoin(Constants.OUTPUT_FILE1,Constants.OUTPUT_FILE2,Constants.FINAL_SORT_JOIN_OUTPUT);
				end = System.nanoTime();
				System.out.println("Sorted Join: " + (end - start) / 1_000_000_000 + " seconds");
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
	
	
}
