package join;

import java.io.IOException;
import java.io.File;
import join.Constants;

public class Main {
	static NestedJoin nesjoin = new NestedJoin();
	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			printHelp();
		} else {
			switch (args[0]) {

			case "1":
				nesjoin.Join(Constants.INPUT_FILE1,Constants.INPUT_FILE2,Constants.OUTPUT_FILE);
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
	
	private static void Join() {
		// TODO Auto-generated method stub
		
	}
}
