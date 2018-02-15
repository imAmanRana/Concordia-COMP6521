/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCKS_IN_MEMORY;
import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.MAIN_MEMORY_SIZE;
import static comp6521.Constants.OUTPUT_FILE1_PATH;
import static comp6521.Constants.OUTPUT_FILE2_PATH;
import static comp6521.Constants.TUPLES_IN_BLOCK;
import static comp6521.Constants.TUPPLES_IN_BUFFER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author AmanRana
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		init();

		// read first file
		
		int noOfTuplesInR1 = sort(new File(Main.class.getResource(Constants.INPUT_FILE1_PATH).getFile()),new File(Constants.OUTPUT_FILE1_PATH));
		
		int noOfTuplesInR2 = sort(new File(Main.class.getResource(Constants.INPUT_FILE2_PATH).getFile()),new File(Constants.OUTPUT_FILE2_PATH));
		
		//implement merge
		int noOfPasses = (int)Math.ceil(Utils.log2((int)Math.ceil((double)noOfTuplesInR1/TUPPLES_IN_BUFFER)));
		int newSubListSize = Constants.TUPPLES_IN_BUFFER;
		String readFile,writeFile;
		
		List<String> list1;
		List<String> list2;
		int linesReadFromList1=0,linesReadFromList2=0;
		for(int i=1;i<=noOfPasses;i++) {
			
			int list1StartPoint=1;
			int list2StartPoint=1+newSubListSize;
			if(i%2==1) {
				readFile = "/output_bag1.txt";
				writeFile = "/Aman.txt";
						
			}else {
				readFile = "/Aman.txt";
				writeFile = "/output_bag1.txt";
			}
			
			
			
			int noOfSubList = (int)Math.ceil((double)noOfTuplesInR1/newSubListSize);
			
			for(int j=1;j<=Math.ceil((double)noOfSubList/2);j++) {
				
				//	int noOfTimesToSubList = (int)Math.ceil((double)newSubListSize/(Constants.TUPPLES_IN_BUFFER/4)); 
				
				list1= Utils.readFromFile(list1StartPoint, new File(Main.class.getResource(readFile).getPath()), Constants.TUPPLES_IN_BUFFER/4);
				list2 = Utils.readFromFile(list1StartPoint+newSubListSize, new File(Main.class.getResource(readFile).getPath()), Constants.TUPPLES_IN_BUFFER/4);
				
				
				linesReadFromList1 += list1.size();
				linesReadFromList2 += list2.size();
				
				int ii = 0, jj = 0;
				List<String> mergedList = new ArrayList<>();
				while(linesReadFromList1<=newSubListSize && linesReadFromList2<=newSubListSize) {
				     
			        // Traverse both array
			        while (ii<list1.size() && jj <list2.size())
			        {
			            // Check if current element of first
			            // array is smaller than current element
			            // of second array. If yes, store first
			            // array element and increment first array
			            // index. Otherwise do same with second array
			        	
			        	if(mergedList.size()==Constants.TUPPLES_IN_BUFFER/2) {
			        		
			        		Utils.write(mergedList, new File("A:\\CodingStuff\\git\\Wontons\\Project1\\resources\\"+writeFile));
			        		mergedList.clear();
			        		
			        	}else {
			        		if (list1.get(ii).compareTo(list2.get(jj))<=0)
				            	mergedList.add(list1.get(ii++));
				            else
				            	mergedList.add(list2.get(jj++));	
			        	}
			            
			        }
			        
			        if(ii==list1.size()) {
			        	list1StartPoint+=Constants.TUPPLES_IN_BUFFER/4;
						list1= Utils.readFromFile(list1StartPoint, new File(Main.class.getResource(readFile).getPath()), Constants.TUPPLES_IN_BUFFER/4);
						ii=0;
						linesReadFromList1+=list1.size();
			        }else if(jj==list2.size()){
			        	list2StartPoint+=Constants.TUPPLES_IN_BUFFER/4;
			        	list2= Utils.readFromFile(list2StartPoint, new File(Main.class.getResource(readFile).getPath()), Constants.TUPPLES_IN_BUFFER/4);
						jj=0;
						linesReadFromList2+=list2.size();
			        }
					
					
				}
				
				
				if(linesReadFromList1<newSubListSize) {
					while(linesReadFromList1<newSubListSize) {
						int emptySpace = TUPPLES_IN_BUFFER-mergedList.size()-list1.size();
						list1.addAll(Utils.readFromFile(list1StartPoint, new File(Main.class.getResource(readFile).getPath()), emptySpace));
						linesReadFromList1+=emptySpace;
					}
				} else {
					while(linesReadFromList2<newSubListSize) {
						int emptySpace = TUPPLES_IN_BUFFER-mergedList.size()-list2.size();
						list2.addAll(Utils.readFromFile(list2StartPoint, new File(Main.class.getResource(readFile).getPath()), emptySpace));
						linesReadFromList2+=emptySpace;
					}
				}
				
				
				list1StartPoint+= newSubListSize;
				list2StartPoint+= newSubListSize;
				
			}
			newSubListSize*=2;
		}
	}

	private static int sort(File inputFile,File outputFile) throws IOException {
		List<String> records;
		int readLine=1;
		int noOfTuples=0;
		do {
			records = Utils.readFromFile(readLine, inputFile,Constants.TUPPLES_IN_BUFFER);

			// sort the records
			Collections.sort(records);

			// write back to file
			Utils.write(records, outputFile);
			readLine+=TUPPLES_IN_BUFFER;
			noOfTuples+=records.size();
		} while (records != null && !records.isEmpty());
		return noOfTuples;
	}

	private static List<String> merge(List<String> list1,List<String> list2) 
	{
		List<String> mergedList = new ArrayList<>();
		
		int i = 0, j = 0;
	     
        // Traverse both array
        while (i<list1.size() && j <list2.size())
        {
            // Check if current element of first
            // array is smaller than current element
            // of second array. If yes, store first
            // array element and increment first array
            // index. Otherwise do same with second array
            if (list1.get(i).compareTo(list2.get(j))<=0)
            	mergedList.add(list1.get(i++));
            else
            	mergedList.add(list2.get(j++));
        }
     
        // Store remaining elements of first array
        while (i < list1.size())
            mergedList.add(list1.get(i++));
     
        // Store remaining elements of second array
        while (j < list2.size())
        	mergedList.add(list2.get(j++));
		
		
		return mergedList;
	}

	private static void init() {
		// read the properties file
		Properties properties = new Properties();
		try(InputStream inputStream = Main.class.getResourceAsStream("/application.properties")) {
			properties.load(inputStream);
			MAIN_MEMORY_SIZE = Integer.valueOf(properties.getProperty("MAIN_MEMORY_SIZE"));
			INPUT_FILE1_PATH = properties.getProperty("INPUT_FILE1_PATH");
			INPUT_FILE2_PATH = properties.getProperty("INPUT_FILE2_PATH");
			OUTPUT_FILE1_PATH = properties.getProperty("OUTPUT_FILE1_PATH");
			OUTPUT_FILE2_PATH = properties.getProperty("OUTPUT_FILE2_PATH");
			BLOCK_SIZE = Integer.valueOf(properties.getProperty("BLOCK_SIZE"));

			BLOCKS_IN_MEMORY = MAIN_MEMORY_SIZE / BLOCK_SIZE;
			TUPPLES_IN_BUFFER = BLOCKS_IN_MEMORY*TUPLES_IN_BLOCK;
			
			
			
			PrintWriter pw = new PrintWriter(new File(Constants.OUTPUT_FILE1_PATH));
			pw.close();
			pw = new PrintWriter(new File(Constants.OUTPUT_FILE2_PATH));
			pw.close();
			

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception");
			e.printStackTrace();
		}
	}

}
