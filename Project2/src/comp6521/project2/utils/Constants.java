package comp6521.project2.utils;

public class Constants {

	public static final String INPUT_FILE1 = "A:\\TEMP\\MiniProject2_6521\\MY_JoinT1.txt";
	public static final String INPUT_FILE2 = "A:\\TEMP\\MiniProject2_6521\\MY_JoinT2.txt";
	public static final String NESTEDJOIN_OUTPUT_FILE = "A:\\TEMP\\MiniProject2_6521\\output.txt";
	public static final String OUTPUT_FILE1 = "A:\\TEMP\\MiniProject2_6521\\output1.txt";
	public static final String OUTPUT_FILE2 = "A:\\TEMP\\MiniProject2_6521\\output2.txt";
	public static final String MERGED_OUTPUT1 = "A:\\TEMP\\MiniProject2_6521\\mergedOutput1.txt";
	public static final String MERGED_OUTPUT2 = "A:\\TEMP\\MiniProject2_6521\\mergedOutput2.txt";
	public static final String FINAL_SORT_JOIN_OUTPUT = "A:\\TEMP\\MiniProject2_6521\\finalsortjoin.txt";
	public static final String GRADES_FILE = "A:\\TEMP\\MiniProject2_6521\\grades.txt";
	
	/*public static final String INPUT_FILE1 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\t1.txt";
	public static final String INPUT_FILE2 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\t2.txt";
	public static final String NESTEDJOIN_OUTPUT_FILE = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\output.txt";
	public static final String OUTPUT_FILE1 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\output1.txt";
	public static final String OUTPUT_FILE2 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\output2.txt";
	public static final String MERGED_OUTPUT1 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\mergedOutput1.txt";
	public static final String MERGED_OUTPUT2 = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\mergedOutput2.txt";
	public static final String FINAL_SORT_JOIN_OUTPUT = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\finalsortjoin.txt";
	public static final String GRADES_FILE = "A:\\CodingStuff\\git\\Wontons\\Project2\\resources\\grades.txt";*/

	/*
	 * MAIN MEMORY SIZE = 5mb MEMORY_UTILIZATION = 75% use this when -Xmx5m
	 * parameter is used for JVM
	 */
	// 5242880
	public static final int MAIN_MEMORY_SIZE = 5242880;
	public static final int MEMORY_UTILIZATION = 70;

	public static final int TUPLE_SIZE_IN_BYTES_T1 = 100;
	public static final int TUPLE_SIZE_IN_BYTES_T2 = 27;
	public static final int TUPLE_SIZE_IN_BYTES_GRADES = 13;
	public static final int TUPLES_IN_BLOCK_T1 = 40;
	public static final int TUPLES_IN_BLOCK_T2 = 130;
	public static final int BLOCK_SIZE = 4096;
	public static final int LINE_SEPARATOR_LENGTH = System.lineSeparator().getBytes().length;
	public static final int STUDENT_ID_LENGTH = 8;

	public static final int TUPPLES_IN_BUFFER_T1_NESTED_JOIN = (int) ((0.25)
			* ((double) MAIN_MEMORY_SIZE / TUPLE_SIZE_IN_BYTES_T1) * ((double) MEMORY_UTILIZATION / 100));

	public static final int TUPPLES_IN_BUFFER_T2_NESTED_JOIN = (int) ((0.25)
			* ((double) MAIN_MEMORY_SIZE / TUPLE_SIZE_IN_BYTES_T2) * ((double) MEMORY_UTILIZATION / 100));

	public static final int TUPPLE_FOR_JOINED_OUTPUT = (int) (((0.40) * ((double) MAIN_MEMORY_SIZE)
			/ ((double) TUPLE_SIZE_IN_BYTES_T1 + TUPLE_SIZE_IN_BYTES_T2 + LINE_SEPARATOR_LENGTH - STUDENT_ID_LENGTH))
			* ((double) MEMORY_UTILIZATION / 100));

	public static final int TUPPLE_FOR_GARDES_OUTPUT = (int) (((0.10) * ((double) MAIN_MEMORY_SIZE)
			/ ((double) TUPLE_SIZE_IN_BYTES_GRADES + LINE_SEPARATOR_LENGTH)) * ((double) MEMORY_UTILIZATION / 100));

	public static final int TUPPLES_IN_BUFFER_T1_SORT = (int) (((double) MAIN_MEMORY_SIZE / TUPLE_SIZE_IN_BYTES_T1)
			* ((double) MEMORY_UTILIZATION / 100));

	public static final int TUPPLES_IN_BUFFER_T2_SORT = (int) (((double) MAIN_MEMORY_SIZE / TUPLE_SIZE_IN_BYTES_T2)
			* ((double) MEMORY_UTILIZATION / 100));

}
