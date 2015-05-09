package org.macau.local.sample;

/**
 * for the textual
 * one data's Set using another set word account to get a approximately result
 * for example:
 * 	R1(A,B,C)
 * it's weight is the sum count of A, B, C in the S set
 * because those value are the candidate value but not the real number of the result
 * 
 * and How to compare the result of sample value and the real sample value
 * 
 */
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class TextualInformation {

	/**
	 * 
	 * @param rRecords
	 * @return the textual tag account information
	 */
	public static Map<Double,Integer> getAccountOfTextual(ArrayList<FlickrData> rRecords){
		
		Map<Double,Integer> accountMap = new HashMap<Double,Integer>();
		
		for(FlickrData record: rRecords){
			
			if(!record.getTextual().equals("null")){
				
				String[] textualList = record.getTextual().split(";");
				
				for(String textual : textualList){
					
					Double key = Double.parseDouble(textual);
					
					if(accountMap.get(key) == null){
						
						accountMap.put(key, new Integer(1));
						
					}else{
						
						int count = accountMap.get(key) + 1;
						accountMap.remove(key);
						accountMap.put(key, count);
						
					}
					
				}
			}
		}
		return accountMap;
	}
	
	public static void main(String[] args){
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		Map<Double,Integer> accountMap = getAccountOfTextual(rRecords);
		
		String outputPath ="D:\\Data\\Flickr Data\\rLat.data";
		try
		{

			FileWriter writer = new FileWriter(outputPath);
			
			Iterator iter = accountMap.entrySet().iterator(); 
			
			while (iter.hasNext()) {
				
			    Map.Entry entry = (Map.Entry) iter.next(); 
			    Object key = entry.getKey(); 
			    Object val = entry.getValue(); 
			    writer.write(key + "\n");
			} 
		
			writer.close();
		}
		
		catch(IOException iox)
		{
			System.out.println("Problemwriting" + outputPath);
		}
		
		String outputPath1 ="D:\\Data\\Flickr Data\\count.data";
		try
		{

			FileWriter writer = new FileWriter(outputPath1);
			
			Iterator iter = accountMap.entrySet().iterator(); 
			
			while (iter.hasNext()) { 
			    Map.Entry entry = (Map.Entry) iter.next(); 
			    Object key = entry.getKey(); 
			    Object val = entry.getValue(); 
			    writer.write(val + "\n");
			} 
			
			writer.close();
		}
		
		catch(IOException iox)
		{
			System.out.println("Problemwriting" + outputPath);
		}
	}
}
