package join;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;

public class Utils {
	public boolean compare(byte[] tuple1,byte[] tuple2)
	{
		for(int i=0;i<8;i++)
		{
			if(tuple1[i]!=tuple2[i])
				return false;
		}
		return true;
	}
	
	public byte [] combine(byte[] tuple1,byte[] tuple2)
	{
		
		byte [] result = new byte[tuple1.length+tuple2.length-9];
		for(int i=0;i<tuple1.length-1;i++)
			{
				result[i]=tuple1[i];
			}
		for(int j=8;j< tuple2.length ;j++)
			result[tuple1.length+j-9] = tuple2[j];
		
		return result;
	}
	
	public boolean createNewFile(File file) throws IOException {

		if (!file.exists()) {
			return file.createNewFile();
		}
		return false;
	}
	
	public void clearFile(File file) throws IOException {

		boolean status = createNewFile(file);
		if (!status) {
			new PrintWriter(file).close();
		}
}
}
