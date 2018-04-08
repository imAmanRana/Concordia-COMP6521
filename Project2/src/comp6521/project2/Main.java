package comp6521.project2;

import java.io.File;
import java.io.IOException;

import comp6521.project2.utils.Constants;
import comp6521.project2.utils.Utils;

public class Main {

	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			printHelp();
		} else {
			switch (args[0]) {

			case "1":
				NestedJoin nestedJoin = new NestedJoin();
				long start = System.nanoTime();
				nestedJoin.join(Constants.INPUT_FILE1, Constants.INPUT_FILE2, Constants.NESTEDJOIN_OUTPUT_FILE);
				long end = System.nanoTime();
				System.out.println("Nested Join: " + (end - start) / 1_000_000_000 + " seconds");
				System.out.println("DONE : "+Constants.NESTEDJOIN_OUTPUT_FILE);
				break;

			case "2":
				Utils.clearFile(new File(Constants.OUTPUT_FILE1));
				Utils.clearFile(new File(Constants.MERGED_OUTPUT1));
				Sort sort = new Sort();
				// do sorting and merging for bag1
				System.out.println("T1 Sorting");
				sort.sortAndMerge(new File(Constants.INPUT_FILE1), new File(Constants.OUTPUT_FILE1),
						new File(Constants.MERGED_OUTPUT1), Constants.TUPLE_SIZE_IN_BYTES_T1,
						Constants.TUPPLES_IN_BUFFER_T1_SORT);
				break;

			case "3":
				Utils.clearFile(new File(Constants.OUTPUT_FILE2));
				Utils.clearFile(new File(Constants.MERGED_OUTPUT2));
				Sort sort1 = new Sort();
				// do sorting and merging for bag2
				System.out.println("T2 Sorting");
				sort1.sortAndMerge(new File(Constants.INPUT_FILE2), new File(Constants.OUTPUT_FILE2),
						new File(Constants.MERGED_OUTPUT2), Constants.TUPLE_SIZE_IN_BYTES_T2,
						Constants.TUPPLES_IN_BUFFER_T2_SORT);
				break;
			case "4":
				Utils.clearFile(new File(Constants.GRADES_FILE));
				SortedJoin sortedjoin = new SortedJoin();
				start = System.nanoTime();
				sortedjoin.sortJoin(Constants.OUTPUT_FILE1, Constants.OUTPUT_FILE2, Constants.FINAL_SORT_JOIN_OUTPUT,Constants.GRADES_FILE);
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
		System.out.println("1 - Nested Join on files :" + Constants.INPUT_FILE1+" & "+Constants.INPUT_FILE2);
		System.out.println("2 - Sorting and Merging for :" + Constants.INPUT_FILE1);
		System.out.println("3 - Sorting and Merging for :" + Constants.INPUT_FILE2);
		System.out.println("4 - Sort Based Join for  :" + Constants.INPUT_FILE1 + " & " + Constants.INPUT_FILE2);
	}

}
