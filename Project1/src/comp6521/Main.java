/**
 * 
 */
package comp6521;

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
		
		mainMemorySize = Integer.parseInt(args[0]);
		
	}

}
