/**
 * 
 */
package comp6521;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author AmanRana
 *
 */
public class BenchmarkPerformance {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		File inputFile = new File("A:/CodingStuff/git/Wontons/Project1/resources/converted_bag1.txt");
		
		
		
		
		for(int i=0;i<10;i++) {
			
			
			System.out.println("\n\n\n_____________________"+i+"_________________________");
			
			
			//linear read
			long start = System.nanoTime();
			List<String> list = Utils.readLinearlyFromFile(1, inputFile, 25000);
			long end = System.nanoTime();
			System.out.println("Time Taken to read linearly(me) (ms) : "+(end-start)/1_000_000);
			
			start = System.nanoTime();
			list = Utils.readFromFile(1, inputFile, 25000);
			end = System.nanoTime();
			System.out.println("Time Taken to read linearly(kamal) (ms) : "+(end-start)/1_000_000);
			
			
			
			
			start = System.nanoTime();
			list = Utils.readLinearlyFromFile(20000, inputFile, 25000);
			end = System.nanoTime();
			System.out.println("Time Taken to read linearly(me) (ms) : "+(end-start)/1_000_000);
			
			start = System.nanoTime();
			list = Utils.readFromFile(20000, inputFile, 25000);
			end = System.nanoTime();
			System.out.println("Time Taken to read linearly(kamal) (ms) : "+(end-start)/1_000_000);
			
		}
		
		
		
		
		
	}

}
