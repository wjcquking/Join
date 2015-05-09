package org.macau.flickr.data;


import java.io.File;
import java.io.IOException;

/**
 * 
 * @author hadoop
 * The Data I get from YYY brothers
 * The form of the data is 
 * Number of Data
 * the time
 * 
 * I want the form data is 
 * ID;lat;Lon;temporal
 * 
 */


public class CleanFlickrData {
	
	
	
	public static void main(String[] args) throws IOException{
		File f1 = new File("d:\\1.txt");
		
		
		try{
			
		}catch(Exception e){
			boolean b = f1.createNewFile();
		}
		
		
		System.out.println(f1.getAbsolutePath());
	}
}
