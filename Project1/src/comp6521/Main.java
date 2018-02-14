/**
 * 
 */
package comp6521;

import static comp6521.Constants.*;

/**
 * @author AmanRana
 *
 */
public class Main {

	public static int mainMemorySize;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args==null || args.length<=0) {
			System.out.println("Please provide the main memory size(in Mb) in command line");
			return;
		}
		
		mainMemorySize = Integer.parseInt(args[0])*1024*1024;
		int bufferSize = mainMemorySize/BLOCK_SIZE;
		int tuplesInBuffer = bufferSize*TUPLES_IN_BLOCK;
	}

}
