package org.macau.local.analysis.time;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

/*
 * 		
 * 
 * 
 * 
 */
public class SpatialRSTime {
	
	
	public static void main(String[] args) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
//		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		
		for (int i = 0; i < rRecords.size(); i++) {
			
			FlickrData value1 = rRecords.get(i);
//			System.out.println(i);
			
		    for (int j = 0; j < sRecords.size(); j++) {
		    	
		    	FlickrData value2 = sRecords.get(j);
		    	
		    	if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
//		    	
		    		firstCount++;
//		    		if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
//		    		
//		    			SecondCount++;
//		    				
//		    			if (FlickrSimilarityUtil.SpatialSimilarity(value1, value2)) {
//		 		        	ThirdCount++;
//		 		            long ridA = value1.getId();
//		 		            long ridB = value2.getId();
//		 		            if (ridA < ridB) {
//		 		                long rid = ridA;
//		 		                ridA = ridB;
//		 		                ridB = rid;
//		 		            }
//		 		            System.out.println(i + "-- " + ThirdCount);
//		 		            writer.write(ridA + "%" + ridB +"\n");
		 		        }
		    			
//		    		}
		            
//		    	}
		    	
		    }
		    
		}
//		writer.close();
		System.out.println(firstCount);
		System.out.println(SecondCount);
		System.out.println(ThirdCount);
	
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
