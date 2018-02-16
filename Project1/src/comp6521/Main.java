/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCKS_IN_MEMORY;
import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE1_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE2_PATH;
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

		//read the configuration file
		init();

		File inputFile1 = new File(Main.class.getResource("/"+Constants.INPUT_FILE1_PATH).getFile());
		File inputFile2 = new File(Main.class.getResource("/"+Constants.INPUT_FILE2_PATH).getFile());
		File outputFile1 = new File(Constants.OUTPUT_FILE1_PATH);
		File outputFile2 = new File(Constants.OUTPUT_FILE2_PATH);
		File intermediateOutputFile1 = new File(Constants.INTERMEDIATE_OUTPUT_FILE1_PATH);
		File intermediateOutputFile2 = new File(Constants.INTERMEDIATE_OUTPUT_FILE2_PATH);
		
		clearFile(outputFile1);
		clearFile(outputFile2);
		clearFile(intermediateOutputFile1);
		clearFile(intermediateOutputFile2);
		
		
		// SUBLIST SORTING
		//file 1
		//int noOfTuplesInR1 = sort(inputFile1,outputFile1);
		//file 2
		int noOfTuplesInR2 = sort(inputFile2,outputFile2);
		
		
		//System.out.println(merge(noOfTuplesInR1,outputFile1,intermediateOutputFile1));
		System.out.println(merge(noOfTuplesInR2,outputFile2,intermediateOutputFile2));
		
	}

	private static void clearFile(File file) throws IOException {
		PrintWriter writer = new PrintWriter(file);
		writer.print("");
		writer.flush();
		writer.close();
	}

	private static int sort(File inputFile,File outputFile) throws IOException {
		List<String> records=null;
		int readLine=1;
		int noOfTuples=0;
		do {
			records = Utils.readFromFile(readLine, inputFile,Constants.TUPPLES_IN_BUFFER);
			
			/*
			 * Using Java's in-build sorting method(its much efficient)
			 */
			Collections.sort(records);

			// write back to file
			Utils.write(records, outputFile);
			readLine+=TUPPLES_IN_BUFFER;
			noOfTuples+=records.size();
		} while (records != null && !records.isEmpty());
		
		return noOfTuples;
	}

	private static File merge(int noOfTuples,File file, File intermediateFile) throws IOException 
	{
		
		int noOfPasses = (int)Math.ceil(Utils.log2((int)Math.ceil((double)noOfTuples/TUPPLES_IN_BUFFER)));
		final int RECORDS_TO_READ = TUPPLES_IN_BUFFER/4;
		File readFile=null;
		File writeFile=null;
		List<String> mergedList = new ArrayList<>();
		//execute the passes
		for(int i=1;i<=noOfPasses;i++) {
			
			int subListSize = Constants.TUPPLES_IN_BUFFER*(int)Math.pow(2, i-1);
			
			//decide which file to use for reading and which to use for writing
			if(i%2==1) {
				readFile = file;
				writeFile = intermediateFile;
			}else {
				readFile = intermediateFile;
				writeFile = file;
			}
			clearFile(writeFile);
			//no. of sublists for this pass
			int noOfSubList = (int)Math.ceil((double)noOfTuples/subListSize);
			
			//reading position of sublists
			int sublist1ReadPosition = 1;
			int sublist2ReadPosition = 1+subListSize;
			int recordsRead1=0;
			int recordsRead2=0;
			List<String> sublist1;
			List<String> sublist2;
			
			//read 2 sublist at a time and merge them
			for(int j=1;j<=noOfSubList;j+=2) {
				
				//do the merging
				int x=0;
				int y=0;
				
				sublist1 = Utils.readFromFile(sublist1ReadPosition, readFile, RECORDS_TO_READ);
				recordsRead1+=sublist1.size();
				sublist1ReadPosition+=sublist1.size();
				
				sublist2 = Utils.readFromFile(sublist2ReadPosition, readFile, RECORDS_TO_READ);
				recordsRead2+=sublist2.size();
				sublist2ReadPosition+=sublist2.size();
				boolean continueLoop=true;
				while(!sublist1.isEmpty() && !sublist2.isEmpty() && recordsRead1<=subListSize && recordsRead2<=subListSize && continueLoop){
					System.out.println(" recordsRead1 "+recordsRead1+" recordsRead2 "+recordsRead2);
					while(x<sublist1.size() && y<sublist2.size()) {
						
						if(sublist1.get(x).compareTo(sublist2.get(y))<=0) {
							mergedList.add(sublist1.get(x++));
						}else {
							mergedList.add(sublist2.get(y++));
						}
						
						if(mergedList.size()==TUPPLES_IN_BUFFER/2) {
							Utils.write(mergedList, writeFile);
							mergedList.clear();
						}
					}
					
					if(x==sublist1.size()) {
						
						if(recordsRead1==subListSize) {
							continueLoop = false;
						}else {
							sublist1= Utils.readFromFile(sublist1ReadPosition, readFile, RECORDS_TO_READ);
							x=0;
							recordsRead1+=sublist1.size();
							sublist1ReadPosition+=RECORDS_TO_READ;
						}
			        }else if(y==sublist2.size()){
			        	
			        	
			        	if(recordsRead2==subListSize) {
			        		continueLoop = false;
			        	}else {
			        		sublist2= Utils.readFromFile(sublist2ReadPosition, readFile, RECORDS_TO_READ);
							y=0;
							recordsRead2+=sublist2.size();
							sublist2ReadPosition+=RECORDS_TO_READ;
			        	}
			        }
				}
				
				//check which list is remaining
				if(recordsRead1<=subListSize && x<sublist1.size()) {
					
					int availableMemorySize = Constants.TUPPLES_IN_BUFFER-mergedList.size()-sublist1.size()+x;
					
					if((subListSize-recordsRead1)>availableMemorySize) {
						mergedList.addAll(sublist1.subList(x, sublist1.size()));
						mergedList.addAll(Utils.readFromFile(sublist1ReadPosition, readFile, availableMemorySize));
						sublist1ReadPosition+=availableMemorySize;
						recordsRead1+=availableMemorySize;
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						
						for(int size = (subListSize-recordsRead1);size>0;size-=TUPPLES_IN_BUFFER) {
							if(size>=TUPPLES_IN_BUFFER) {
								sublist1 = Utils.readFromFile(sublist1ReadPosition, readFile, TUPPLES_IN_BUFFER);
								sublist1ReadPosition+=TUPPLES_IN_BUFFER;
								recordsRead1+=sublist1.size();
							}else {
								sublist1 = Utils.readFromFile(sublist1ReadPosition, readFile, size);
								sublist1ReadPosition+=size;
								recordsRead1+=sublist1.size();
							}
							Utils.write(sublist1, writeFile);
						}
						
					}else {
						
						mergedList.addAll(sublist1.subList(x, sublist1.size()));
						sublist1 = Utils.readFromFile(sublist1ReadPosition, readFile, (subListSize-recordsRead1));
						mergedList.addAll(sublist1);
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						recordsRead1+=sublist1.size();
						sublist1ReadPosition+=(subListSize-recordsRead1);
						
					}
				}else if(recordsRead2<=subListSize && y<sublist2.size()) {
					int availableMemorySize = Constants.TUPPLES_IN_BUFFER-mergedList.size()-sublist2.size()+y;
					
					if((subListSize-recordsRead2)>availableMemorySize) {
						mergedList.addAll(sublist2.subList(y, sublist2.size()));
						mergedList.addAll(Utils.readFromFile(sublist2ReadPosition, readFile, availableMemorySize));
						sublist2ReadPosition+=availableMemorySize;
						recordsRead2+=availableMemorySize;
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						
						for(int size = (subListSize-recordsRead2);size>0;size-=TUPPLES_IN_BUFFER) {
							if(size>=TUPPLES_IN_BUFFER) {
								sublist2 = Utils.readFromFile(sublist2ReadPosition, readFile, TUPPLES_IN_BUFFER);
								sublist2ReadPosition+=TUPPLES_IN_BUFFER;
								recordsRead2+=sublist2.size();
							}else {
								sublist2 = Utils.readFromFile(sublist2ReadPosition, readFile, size);
								sublist2ReadPosition+=size;
								recordsRead2+=sublist2.size();
							}
							Utils.write(sublist2, writeFile);
						}
						
					}else {
						mergedList.addAll(sublist2.subList(y, sublist2.size()));
						sublist2 = Utils.readFromFile(sublist2ReadPosition, readFile, (subListSize-recordsRead2));
						mergedList.addAll(sublist2);
						Utils.write(mergedList, writeFile);
						mergedList.clear();
						recordsRead2+=sublist2.size();
						sublist2ReadPosition+=(subListSize-recordsRead2);
					}
				}
				
				sublist1ReadPosition+=subListSize;
				sublist2ReadPosition+=subListSize;
				recordsRead1=0;
				recordsRead2=0;
				mergedList.clear();
				
			}
			
			
		}
		return writeFile;
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
			INTERMEDIATE_OUTPUT_FILE1_PATH = properties.getProperty("INTERMEDIATE_OUTPUT_FILE1_PATH");
			INTERMEDIATE_OUTPUT_FILE2_PATH = properties.getProperty("INTERMEDIATE_OUTPUT_FILE2_PATH");
					
			BLOCK_SIZE = Integer.valueOf(properties.getProperty("BLOCK_SIZE"));

			BLOCKS_IN_MEMORY = MAIN_MEMORY_SIZE / BLOCK_SIZE;
			TUPPLES_IN_BUFFER = BLOCKS_IN_MEMORY*TUPLES_IN_BLOCK;
			

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception");
			e.printStackTrace();
		}
	}

}
