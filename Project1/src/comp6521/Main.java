/**
 * 
 */
package comp6521;

import static comp6521.Constants.BLOCKS_IN_MEMORY;
import static comp6521.Constants.BLOCK_SIZE;
import static comp6521.Constants.FINAL_OUTPUT;
import static comp6521.Constants.INPUT_FILE1_PATH;
import static comp6521.Constants.INPUT_FILE2_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE1_PATH;
import static comp6521.Constants.INTERMEDIATE_OUTPUT_FILE2_PATH;
import static comp6521.Constants.MAIN_MEMORY_SIZE;
import static comp6521.Constants.OUTPUT_FILE1_PATH;
import static comp6521.Constants.OUTPUT_FILE2_PATH;
import static comp6521.Constants.TUPLES_IN_BLOCK;
import static comp6521.Constants.TUPLE_SIZE_IN_BYTES;
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

		File inputFile1 = new File(Constants.INPUT_FILE1_PATH);
		File inputFile2 = new File(Constants.INPUT_FILE2_PATH);
		File outputFile1 = new File(Constants.OUTPUT_FILE1_PATH);
		File outputFile2 = new File(Constants.OUTPUT_FILE2_PATH);
		File intermediateOutputFile1 = new File(Constants.INTERMEDIATE_OUTPUT_FILE1_PATH);
		File intermediateOutputFile2 = new File(Constants.INTERMEDIATE_OUTPUT_FILE2_PATH);
		File finalOutput = new File(Constants.FINAL_OUTPUT);
		
		clearFile(outputFile1);
		clearFile(outputFile2);
		clearFile(intermediateOutputFile1);
		clearFile(intermediateOutputFile2);
		clearFile(finalOutput);
		
		/*
		// SUBLIST SORTING
		//file 1
		int noOfTuplesInR1 = sort(inputFile1,outputFile1);
		//file 2
		int noOfTuplesInR2 = sort(inputFile2,outputFile2);
		
		
		File sortedFile1 = merge(noOfTuplesInR1,outputFile1,intermediateOutputFile1);
		File sortedFile2 = merge(noOfTuplesInR2,outputFile2,intermediateOutputFile2);
		
		
		*/
		
		/*REMOVE CODE, JUST FOR TESTING */
		int noOfTuplesInR1 = 12;
		int noOfTuplesInR2 = 24;
		File sortedFile1 = new File("A:\\CodingStuff\\git\\Wontons\\Project1\\resources\\Temp9.txt");
		File sortedFile2 = new File("A:\\CodingStuff\\git\\Wontons\\Project1\\resources\\Temp10.txt");
		/*REMOVE CODE, JUST FOR TESTING */
		
		performBagDifference(sortedFile1,noOfTuplesInR1,sortedFile2,noOfTuplesInR2,finalOutput);
		
		
	}

	private static void performBagDifference(File sortedFile1, int noOfTuplesInR1, File sortedFile2, int noOfTuplesInR2,File finalOutput) throws IOException {
		
		final int RECORDS_TO_READ = (noOfTuplesInR1<TUPPLES_IN_BUFFER/3)?noOfTuplesInR1:TUPPLES_IN_BUFFER/3;
		
		int position1 = 1;
		int start=1;
		int end=noOfTuplesInR2;
		List<String> sublist1;
		List<String> output=new ArrayList<String>();
		int duplicates1=0;
		int duplicates2=0;
		int duplicates2_1=0;
		int duplicates2_2=0;
		boolean isSearchAbove = true;
		String record;
		int tuplesRemainingToRead=-1;
		
		while(position1<=noOfTuplesInR1) {
			
			sublist1 = Utils.readFromFile(position1, sortedFile1,RECORDS_TO_READ);
			position1+=RECORDS_TO_READ;

			for(int i=0;i<sublist1.size();i++) {
				//check for duplicates in list1
				duplicates1=1;
				duplicates2_1=0;
				duplicates2_2=0;
				duplicates2=0;
				record = sublist1.get(i);
				//check if the tuples below are same
				while(i<sublist1.size()-1 && record.compareTo(sublist1.get(i+1))==0) {
					i++;
					duplicates1++;
					if(i==sublist1.size()-1) {
						
						if(position1>noOfTuplesInR1) {
							break;
						}
						
						sublist1 = Utils.readFromFile(position1, sortedFile1, RECORDS_TO_READ);
						position1+=RECORDS_TO_READ;
						i=-1;
					}
				}
				
				
				int position = performBinarySearchOnFile(record,sortedFile2,start,end);
				
				if(position!=-1) {
					
					//check records above this position
					isSearchAbove = true;
					tuplesRemainingToRead = position-start;
					duplicates2_1+=linearSearchForDuplicates(record,position,sortedFile2,tuplesRemainingToRead,RECORDS_TO_READ,isSearchAbove,duplicates1);
					

					// check records below this position
					isSearchAbove = false;
					tuplesRemainingToRead = noOfTuplesInR2 - position;
					duplicates2_2 += linearSearchForDuplicates(record, position + 1, sortedFile2,
							tuplesRemainingToRead, RECORDS_TO_READ, isSearchAbove, duplicates1);
					start = position + duplicates2_2+1;
					
					
					if (duplicates2_1 > 0 || duplicates2_2 > 0) {
						duplicates2 = duplicates2_1 + duplicates2_2 + 1; 	// add the record found at position
					}else {
						duplicates2=1;
					}
				}
				
				
				
				if(output.size()==TUPPLES_IN_BUFFER/3) {
					Utils.write(output, finalOutput);
					output.clear();
				}
				if(duplicates1>=duplicates2) {
					output.add(record+": "+(duplicates1-duplicates2));
				}else {
					output.add(record+": 0");
				}
			}
		}
		
		Utils.write(output, finalOutput);
		output.clear();
		System.out.println(finalOutput);
		
	}

	private static int performBinarySearchOnFile(String recordOfFile1, File file, int start,int end) {
		
		int returnValue = -1;
		int mid;
		List<String> recordOfFile2;
		
		while(start<=end) {
			mid = (start+end)/2;
			recordOfFile2 = Utils.readFromFile(mid, file, 1);
			
			if(recordOfFile2.isEmpty()) {
				break;
			}else if(recordOfFile1.compareTo(recordOfFile2.get(0))==0) {
				returnValue = mid;
				break;
			}else if(recordOfFile1.compareTo(recordOfFile2.get(0))>0) {
				start=mid+1;
			}else {
				end = mid-1;
			}
			recordOfFile2.clear();
		}
		return returnValue;
	}

	private static int linearSearchForDuplicates(String list1Record, int position, File file, int noOftuplesRemaining,int recordsToRead, boolean searchAbove,int duplicates1) {

		int start = position;
		int duplicates = 0;
		List<String> list;

		if (searchAbove) {
			if (noOftuplesRemaining - recordsToRead > 0) {
				start = position - recordsToRead;
			} else {
				start = position-noOftuplesRemaining;
				recordsToRead = noOftuplesRemaining;
			}
		}
		
		
		
		list = Utils.readFromFile(start, file, recordsToRead);
		noOftuplesRemaining -= recordsToRead;
		
		
		
		
		if (searchAbove) {
			
			if(noOftuplesRemaining>recordsToRead) {
				start = start-recordsToRead;
			}else {
				start = start-noOftuplesRemaining;
				recordsToRead=noOftuplesRemaining;
			}

		} else {
			start += recordsToRead;
		}
		for (int i = 0; i < list.size(); i++) {
			
			if (list1Record.compareTo(list.get(i)) == 0) {
				duplicates++;
				
				/*if(duplicates1<duplicates) {
					break;
				}*/
				
				if (i == list.size() - 1) {
					
					if (noOftuplesRemaining > 0) {

						list = Utils.readFromFile(start, file, recordsToRead);
						noOftuplesRemaining -= recordsToRead;
						i = -1;
						if (searchAbove) {
							start = noOftuplesRemaining - recordsToRead > 1 ? start - recordsToRead : 1;

						} else {
							start = (start + recordsToRead) > noOftuplesRemaining ? start + recordsToRead
									: start + noOftuplesRemaining;
						}
					}
				}
			}else {
				break;
			}
		}

		return duplicates;
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
			FINAL_OUTPUT = properties.getProperty("FINAL_OUTPUT");
					
			BLOCK_SIZE = Integer.valueOf(properties.getProperty("BLOCK_SIZE"));
			TUPLE_SIZE_IN_BYTES = Integer.valueOf(properties.getProperty("TUPLE_SIZE_IN_BYTES"));
			TUPLES_IN_BLOCK = Integer.valueOf(properties.getProperty("TUPLES_IN_BLOCK"));

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
