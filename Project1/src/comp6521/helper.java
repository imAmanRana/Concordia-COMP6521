package comp6521;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class helper {

	public ArrayList<String> readBag1() throws IOException {
		
		readBag1 = new BufferedReader(new InputStreamReader(new FileInputStream(BAG1_FILE_PATH)));
		ArrayList<String> bag1 = new ArrayList<>();
		String input = "";
		
		try {
			
			while ((input = readBag1.readLine()) != null) {
				
				bag1.add(input);
			}
						
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			
		}
		
		return bag1;
	}
	
	public ArrayList<String> readBag2() throws IOException {
		
		readBag2 = new BufferedReader(new InputStreamReader(new FileInputStream(BAG2_FILE_PATH)));
		ArrayList<String> bag2 = new ArrayList<>();
		String input = "";
		
		try {
			
			while ((input = readBag2.readLine()) != null) {
				
				bag2.add(input);
			}
						
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			
		}
		
		return bag2;
	}
	
	private BufferedReader readBag1;
	private BufferedReader readBag2;
	private final String BAG1_FILE_PATH = "src\\resources\\bag1.txt";
	private final String BAG2_FILE_PATH = "src\\resources\\bag2.txt";
}
