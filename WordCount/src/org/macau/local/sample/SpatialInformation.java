package org.macau.local.sample;

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

/**
 * 
 * @author mb25428
 * Create a way to evaluate the influence of the calculation
 */

public class SpatialInformation {

	public static void main(String[] args){
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,1);
		
		List<Double> latList = new ArrayList<Double>();
		
		Map<Double,Integer> accountMap = new HashMap<Double,Integer>();
		
		for(FlickrData record: rRecords){
			
			Double key = record.getLon();
			if(accountMap.get(key) == null){
				accountMap.put(key, new Integer(1));
			}else{
				int count = accountMap.get(key) + 1;
				accountMap.remove(key);
				accountMap.put(key, count);
			}
		}
		
		
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
