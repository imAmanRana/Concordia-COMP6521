/**
 * 
 */
package comp6251;

/**
 * @author Amandeep Singh
 *
 */
public class Constants {

	public static final int MAIN_MEMORY = 5*1024*1024;
	public static final int MEMORY_UTILIZATION = 70;
	public static final int TUPPLE_SIZE_T1 = 100;
	public static final int TUPPLE_SIZE_T2 = 27;
	public static final int TUPPLE_SIZE_GRADES = 13;
	public static final int BLOCK_SIZE = 4096;
	public static final int LINE_SEPARATOR_LENGTH = System.lineSeparator().getBytes().length;
	
//	public static final String INPUT_FILE1 = "A:\\CodingStuff\\Eclipse_Workspaces\\AdvDatabase\\BigProject\\resources\\MY_JoinT1.txt";
//	public static final String INPUT_FILE2 = "A:\\CodingStuff\\Eclipse_Workspaces\\AdvDatabase\\BigProject\\resources\\MY_JoinT2.txt";
//	public static final String NESTEDJOIN_OUTPUT_FILE = "A:\\CodingStuff\\Eclipse_Workspaces\\AdvDatabase\\BigProject\\resources\\nestedOutput.txt";

	public static final String INPUT_FILE1 = "A:\\TEMP\\MiniProject2_6521\\MY_JoinT1.txt";
	public static final String INPUT_FILE2 = "A:\\TEMP\\MiniProject2_6521\\MY_JoinT2.txt";
	public static final String INTERMEDIATE_T1 = "A:\\TEMP\\MiniProject2_6521\\intermediateT1.txt";
	public static final String INTERMEDIATE_T2 = "A:\\TEMP\\MiniProject2_6521\\intermediateT2.txt";
	public static final String SORTED_T1 = "A:\\TEMP\\MiniProject2_6521\\sortedT1.txt";
	public static final String SORTED_T2 = "A:\\TEMP\\MiniProject2_6521\\sortedT2.txt";
	public static final String NESTEDJOIN_OUTPUT_FILE = "A:\\TEMP\\MiniProject2_6521\\nestedJoin.txt";
	public static final String GRADES_FILE = "A:\\TEMP\\MiniProject2_6521\\grades.txt";
	public static final String SORTEDJOIN_OUTPUT_FILE = "A:\\TEMP\\MiniProject2_6521\\sortedJoin.txt";
	
	public static final int STUDENT_ID_LENGTH = 8; 
	public static final int TUPPLE_SIZE_T3 = TUPPLE_SIZE_T1+TUPPLE_SIZE_T2-STUDENT_ID_LENGTH;
	public static final int AVAILABLE_MEMORY = (int)(MAIN_MEMORY*((double)MEMORY_UTILIZATION/100));
	public static final int T1_TUPPLES_IN_BLOCK = BLOCK_SIZE/TUPPLE_SIZE_T1;
	public static final int T2_TUPPLES_IN_BLOCK = BLOCK_SIZE/TUPPLE_SIZE_T2;
	public static final int T3_TUPPLES_IN_BLOCK = BLOCK_SIZE/TUPPLE_SIZE_T3;
	public static final int GRADE_TUPPLES_IN_BLOCK = BLOCK_SIZE/TUPPLE_SIZE_GRADES;
	public static final int T1_TUPPLES_IN_BUFFER_FOR_NESTED = (int)((.40)*T1_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int T2_TUPPLES_IN_BUFFER_FOR_NESTED = (int)((.40)*T2_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int T3_TUPPLES_IN_BUFFER_FOR_NESTED = (int)((.20)*T3_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	
	public static final int T1_TUPPLES_IN_BUFFER_FOR_SORT = (int)(T1_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int T2_TUPPLES_IN_BUFFER_FOR_SORT = (int)((.60)*(T2_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE)));
	
	public static final int T1_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN = (int)((.30)*T1_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int T2_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN = (int)((.30)*T2_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int T3_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN = (int)((.30)*T3_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	public static final int GRADE_TUPPLES_IN_BUFFER_FOR_SORTEDJOIN = (int)((.10)*GRADE_TUPPLES_IN_BLOCK*((double)AVAILABLE_MEMORY/BLOCK_SIZE));
	
}

