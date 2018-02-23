package comp6521;
/**
 * 
 */

/**
 * @author AmanRana
 *
 */
public class Constants {
	
	private Constants() {
	}

	/* CHANGE AS PER YOUR LOCAL MACHINE */
	public static final String INPUT_FILE1 = "/Users/kamal/git/Wontons/Project1/resources/bag1.txt";
	public static final String INPUT_FILE2 = "/Users/kamal/git/Wontons/Project1/resources/bag2.txt";
	public static final String OUTPUT_FILE1 = "/Users/kamal/git/Wontons/Project1/resources/output1.txt";
	public static final String OUTPUT_FILE2 = "/Users/kamal/git/Wontons/Project1/resources/output2.txt";
	public static final String MERGED_OUTPUT1 = "/Users/kamal/git/Wontons/Project1/resources/mergedOutput1.txt";
	public static final String MERGED_OUTPUT2 = "/Users/kamal/git/Wontons/Project1/resources/mergedOutput2.txt";
	public static final String FINAL_OUTPUT = "/Users/kamal/git/Wontons/Project1/resources/finalOutput.txt";
	
//	public static final String INPUT_FILE1 = "/Users/kamal/git/Wontons/Project1/resources/Temp8.txt";
//	public static final String INPUT_FILE2 = "/Users/kamal/git/Wontons/Project1/resources/Temp9.txt";
//	public static final int TUPLE_SIZE_IN_BYTES = 100;
//	public static final int TUPLES_IN_BLOCK = 2;
//	public static final int BLOCK_SIZE = 200;
//	public static final int TUPPLES_IN_BUFFER = 4;

	/* 
	 *	MAIN MEMORY SIZE = 5mb
	 *	MEMORY_UTILIZATION = 75% 
	 *	use this when -Xmx5m parameter is used for JVM
	 */
//	public static final int MAIN_MEMORY_SIZE = 5242880;
//	public static final int MEMORY_UTILIZATION = 75;
	
	
	/* 
	 *	MAIN MEMORY SIZE = 10mb
	 *	MEMORY_UTILIZATION = 60% 
	 *	use this when -Xmx10m parameter is used for JVM
	 */
	public static final int MAIN_MEMORY_SIZE = 10485760;
	public static final int MEMORY_UTILIZATION = 60;
	
	public static final int TUPLE_SIZE_IN_BYTES = 100;
	public static final int TUPLES_IN_BLOCK = 40;
	public static final int BLOCK_SIZE = 4096;
	public static final int TUPPLES_IN_BUFFER = (int)(TUPLES_IN_BLOCK*((double)MAIN_MEMORY_SIZE/BLOCK_SIZE)
																					*((double)MEMORY_UTILIZATION/100));
	public static final int TUPPLE_IN_BAG_DIFF = (int)(TUPPLES_IN_BUFFER/(double)40/100);
}
