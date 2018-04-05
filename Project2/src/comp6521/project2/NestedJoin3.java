package comp6521.project2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import comp6521.project2.utils.Constants;
import comp6521.project2.utils.Utils;

public class NestedJoin3 {
	int output_counter = 0;
	
	
	public void Join(String inputFile1, String inputFile2, String output_file) throws IOException {
		int startPointer1 = 0;
		int startPointer2 = 0;

		ByteBuffer output = ByteBuffer.allocateDirect(Constants.TUPPLE_FOR_JOINED_OUTPUT
				* (Constants.TUPLE_SIZE_IN_BYTES_T1 + Constants.TUPLE_SIZE_IN_BYTES_T2 - 7));
		
		WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(output_file));

		System.out.println(Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN + "  "
				+ Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN + "  " + Constants.TUPPLE_FOR_JOINED_OUTPUT);
		
		
		int lineSeparatorLength = System.lineSeparator().getBytes().length;

		byte fileContent1[] ;
		
		byte fileContent2[] ;
		
		File file = new File(inputFile1);
		File file2 = new File(inputFile2);
		FileInputStream fin = null;
		FileInputStream fin2 = null;
		try {
			// create FileInputStream object
			fin = new FileInputStream(file);
			fin2 = new FileInputStream(file2);
		} catch (IOException ioe) {
			System.out.println("Exception while reading file " + ioe);

		}
	

		
		
System.out.println((double)file.length());
System.out.println(file2.length());
	for(int i = 0; i< file.length(); i+=Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN)
		{
		System.out.println("i"+i);
		fileContent1  = new byte[(int) Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN];
		fin.read(fileContent1);
		startPointer1 = startPointer1 + Constants.TUPPLES_IN_BUFFER_T1_NESTED_JOIN;
		
		for(int j = 0; j< file2.length(); j+=Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN )
		{
			System.out.println("j"+j);
			fileContent2 = new byte[(int) Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN];
		
		fin2.read(fileContent2);
		startPointer2 = startPointer2 + Constants.TUPPLES_IN_BUFFER_T2_NESTED_JOIN;
	
	int output_counter =0;
		for(int ii = 0;ii< fileContent2.length && ii<file2.length();ii+=28)
	{
		byte [] a = Arrays.copyOfRange(fileContent2, ii, ii+28);
		
		for(int jj= 0;jj<fileContent1.length&&jj<file.length();jj+=101)
		{
			byte [] b = Arrays.copyOfRange(fileContent1, jj, jj+101);
			if(a!=null&& b!=null)
			{
			if(Utils.compare(a,b))
			{
				if(output_counter != Constants.TUPPLE_FOR_JOINED_OUTPUT)
				{
				output.put(combine(b,a));
				System.out.println("oc"+output_counter);
				output_counter++;
				}
				else
				{
					output.flip();
					outChannel.write(output);
					output.clear();
					System.out.println("output written");
					output_counter=0;
				}
		}
			}
	}
		
		}
		
		}
	
	}
	output.flip();
	outChannel.write(output);
	outChannel.close();
	output.clear();
	System.out.println("outer output written");
	output_counter=0;
	}
	
	public ByteBuffer combine(byte[] tuple1, byte[] tuple2) {
		byte[] result = new byte[tuple1.length + tuple2.length - 9];
		for (int i = 0; i < tuple1.length - 1; i++) {
			result[i] = tuple1[i];
		}
		for (int j = 8; j < tuple2.length; j++)
			result[tuple1.length + j - 9] = tuple2[j];
		//System.out.println(new String(result));
		return ByteBuffer.wrap(result);
	}

	public boolean compare(byte[] tuple1, byte[] tuple2) {
		for (int i = 0; i < 8; i++) {
			if (tuple1[i] != tuple2[i])
				return false;
		}
		return true;
	}
	}

