package join;

public class Constants 
{
	public static final String INPUT_FILE1 = "/Users/kamal/git/Wontons/Project2/resources/JoinT1.txt";
	public static final String INPUT_FILE2 = "/Users/kamal/git/Wontons/Project2/resources/JoinT2.txt";
	public static final String OUTPUT_FILE = "/Users/kamal/git/Wontons/Project2/resources/output.txt";
	public static final String OUTPUT_FILE1 = "/Users/kamal/git/Wontons/Project1/resources/output1.txt";
	public static final String OUTPUT_FILE2 = "/Users/kamal/git/Wontons/Project1/resources/output2.txt";
	public static final String MERGED_OUTPUT1 = "/Users/kamal/git/Wontons/Project1/resources/mergedOutput1.txt";
	public static final String MERGED_OUTPUT2 = "/Users/kamal/git/Wontons/Project1/resources/mergedOutput2.txt";
	public static final String FINAL_OUTPUT = "/Users/kamal/git/Wontons/Project1/resources/finalOutput.txt";

	
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
	
	public static final int TUPLE_SIZE_IN_BYTES_T1 = 100;
	public static final int TUPLE_SIZE_IN_BYTES_T2 = 27;
	public static final int TUPLES_IN_BLOCK_T1 = 40;
	public static final int TUPLES_IN_BLOCK_T2 = 130;
	public static final int BLOCK_SIZE = 4096;
	public static final int TUPPLES_IN_BUFFER_T1 = (int) (((double)0.33)*((double)MAIN_MEMORY_SIZE/TUPLE_SIZE_IN_BYTES_T1));
															
	public static final int TUPPLES_IN_BUFFER_T2 = (int) (((double)0.33)*((double)MAIN_MEMORY_SIZE/TUPLE_SIZE_IN_BYTES_T2));
	public static final int TUPPLE_FOR_JOINED_OUTPUT = (int)(((double)0.33)*((double)MAIN_MEMORY_SIZE)/((double)TUPLE_SIZE_IN_BYTES_T1+TUPLE_SIZE_IN_BYTES_T2-8));

}
