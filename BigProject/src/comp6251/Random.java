/**
 * 
 */
package comp6251;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author AmanRana
 *
 */
public class Random {

//	public static String INPUT="A:\\TEMP\\MiniProject2_6521\\regression\\MY_JoinT2.txt";
//	public static String OUTPUT="A:\\TEMP\\MiniProject2_6521\\regression\\csvT2.txt";
	
	public static String INPUT="A:\\TEMP\\MiniProject2_6521\\regression\\csvT2.csv";
	public static String OUTPUT="A:\\TEMP\\MiniProject2_6521\\regression\\csvT2_new.txt";
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main1(String[] args) throws IOException {
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(INPUT));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(OUTPUT))){
			
			ByteBuffer inBuffer = ByteBuffer.allocateDirect(1024*1024
					* (Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer outBuffer = ByteBuffer.allocateDirect(1024*1024
					* (120 + Constants.LINE_SEPARATOR_LENGTH));
			byte[] receive = new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];
			StringBuilder s;
			while(inChannel.read(inBuffer)>0) {
				inBuffer.flip();
				while(inBuffer.hasRemaining()) {
					inBuffer.get(receive);
					s = new StringBuilder(new String(receive));
					/*s.insert(0, "\"");
					s.insert(9, "\",\"");
					s.insert(20, "\",\"");
					s.insert(24, "\",\"");
					s.insert(31, "\",\"");
					s.insert(36, "\",\"");
					s.insert(43, "\"");*/
					
					s.insert(9,"\",\"");
					s.insert(22,"\",\"");
					s.insert(35,"\",\"");
					s.insert(41,"\",\"");
					s.insert(47,"\",\"");
					s.insert(59,"\",\"");
					s.insert(119,"\"");
					
					if (outBuffer.position() == outBuffer.capacity()) {
						outBuffer.flip();
						outChannel.write(outBuffer);
						outBuffer.clear();
					}
					outBuffer.put(s.toString().getBytes());
					receive = new byte[Constants.TUPPLE_SIZE_T1 + Constants.LINE_SEPARATOR_LENGTH];
				}
				inBuffer.clear();
			}
			if (outBuffer.position() >0) {
				outBuffer.flip();
				outChannel.write(outBuffer);
				outBuffer.clear();
			}
			
		}
				
	}
	
	public static void main2(String[] args) throws IOException{
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(INPUT));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(OUTPUT))){
			
			ByteBuffer inBuffer = ByteBuffer.allocateDirect(1024*1024
					* (120 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer outBuffer = ByteBuffer.allocateDirect(1024*1024
					* (149 + Constants.LINE_SEPARATOR_LENGTH));
			byte[] receive = new byte[120 + Constants.LINE_SEPARATOR_LENGTH];
			StringBuilder s;
			while(inChannel.read(inBuffer)>0) {
				inBuffer.flip();
				while(inBuffer.hasRemaining()) {
					inBuffer.get(receive);
					s = new StringBuilder(new String(receive));
					/*s.insert(0, "\"");
					s.insert(9, "\",\"");
					s.insert(20, "\",\"");
					s.insert(24, "\",\"");
					s.insert(31, "\",\"");
					s.insert(36, "\",\"");
					s.insert(43, "\"");*/
					
					
					s.insert(0, "insert into student values(");
					s.insert(147,");");
					
					if (outBuffer.position() == outBuffer.capacity()) {
						outBuffer.flip();
						outChannel.write(outBuffer);
						outBuffer.clear();
					}
					outBuffer.put(s.toString().getBytes());
					receive = new byte[120 + Constants.LINE_SEPARATOR_LENGTH];
				}
				inBuffer.clear();
			}
			if (outBuffer.position() >0) {
				outBuffer.flip();
				outChannel.write(outBuffer);
				outBuffer.clear();
			}
			
		}
	}
	
	
	
	public static void main(String[] args) throws IOException{
		try (ReadableByteChannel inChannel = Channels.newChannel(new FileInputStream(INPUT));
				WritableByteChannel outChannel = Channels.newChannel(new FileOutputStream(OUTPUT))){
			
			ByteBuffer inBuffer = ByteBuffer.allocateDirect(1024*1024
					* (44 + Constants.LINE_SEPARATOR_LENGTH));
			ByteBuffer outBuffer = ByteBuffer.allocateDirect(1024*1024
					* (76 + Constants.LINE_SEPARATOR_LENGTH));
			byte[] receive = new byte[44 + Constants.LINE_SEPARATOR_LENGTH];
			StringBuilder s;
			while(inChannel.read(inBuffer)>0) {
				inBuffer.flip();
				while(inBuffer.hasRemaining()) {
					inBuffer.get(receive);
					s = new StringBuilder(new String(receive));
					/*s.insert(0, "\"");
					s.insert(9, "\",\"");
					s.insert(20, "\",\"");
					s.insert(24, "\",\"");
					s.insert(31, "\",\"");
					s.insert(36, "\",\"");
					s.insert(43, "\"");*/
					
					
					s.insert(0, "insert into enrollment values(");
					s.insert(73,");");
					
					if (outBuffer.position() == outBuffer.capacity()) {
						outBuffer.flip();
						outChannel.write(outBuffer);
						outBuffer.clear();
					}
					outBuffer.put(s.toString().getBytes());
					receive = new byte[44 + Constants.LINE_SEPARATOR_LENGTH];
				}
				inBuffer.clear();
			}
			if (outBuffer.position() >0) {
				outBuffer.flip();
				outChannel.write(outBuffer);
				outBuffer.clear();
			}
			
		}
	}

}
