package org.macau.flickr.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 
 * @author hadoop
 * @desc this file is for englarge the data
 * because the data is already been disposed, so the number is large but the size is small
 * there only one machine to read the data from the dfs, so I want to enlarge the data by 
 * adding some unnecessary information to the data
 * 
 */
public class EnlargeFlickrData {

	
	public static void readFile(String fileName){
		
		
		
	}
	
	public static void writeFile(String fileName){
		
	}
	public static void main(String[] args){
		try {
            // read file content from file
            StringBuffer sb= new StringBuffer("");
           
//            FileReader reader = new FileReader("c://test.txt");
            FileReader reader = new FileReader("//home//hadoop//Dropbox//hadoop//input//flickr.data");
            BufferedReader br = new BufferedReader(reader);
           
            String str = null;
           
            int count = 0;
            while((str = br.readLine()) != null) {
            	count++;
            	if(count < 60000 && count %2 == 0){
                  sb.append(str +"\r\n");
            	}
                 
                  //System.out.println(str);
            }
           
            br.close();
            reader.close();
           
            // write string to file
            FileWriter writer = new FileWriter("//home//hadoop//Dropbox//hadoop//input//flickr.odd.data");
            BufferedWriter bw = new BufferedWriter(writer);
            bw.write(sb.toString());
           
            bw.close();
            System.out.println("Over");
            writer.close();
      }
      catch(FileNotFoundException e) {
                  e.printStackTrace();
            }
            catch(IOException e) {
                  e.printStackTrace();
            }
	}
}
