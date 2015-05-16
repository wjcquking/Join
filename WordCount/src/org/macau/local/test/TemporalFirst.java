package org.macau.local.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.macau.flickr.temporal.TemporalUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.util.SimilarityUtil;

public class TemporalFirst {
	
	
	/**
	 * TSO: Temporal Spatial Textual
	 */
	public static void main(String[] args) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		File file = new File("D:\\part-r-00000");
		
		BufferedReader reader = null;
		
		String result = "";
		char spe = ' ';
		try{
			reader = new BufferedReader(new FileReader(file));
	
			
			
			String tempString = null;
			
			while((tempString = reader.readLine()) != null){
				result += tempString + ":";
				spe = tempString.toCharArray()[tempString.length()-1];
			}
		}catch(Exception e){
			
		}
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		int count = 0;
//		System.out.println(result);
		
		String real = "";
		
		
		for (int i = 0; i < rRecords.size(); i++) {
//			System.out.println(i);
			FlickrData rec1 = rRecords.get(i);
//			System.out.println(i + " " + rec1.getLat());
			writer.write(rec1.getLon() + "\n");
			
		    for (int j = 0; j < sRecords.size(); j++) {
		    	
		    	FlickrData rec2 = sRecords.get(j);
		    	
		    		if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    	
		    		firstCount++;
		    		if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		
		    		
		    			SecondCount++;
		    				
		    			if (!rec1.getTextual().equals("null") && !rec2.getTextual().equals("null") && FlickrSimilarityUtil.TextualSimilarity(rec1, rec2)) {
		 		        	ThirdCount++;
		 		            long ridA = rec1.getId();
		 		            long ridB = rec2.getId();
		 		            if (ridA < ridB) {
		 		                long rid = ridA;
		 		                ridA = ridB;
		 		                ridB = rid;
		 		            }
//		 		            writer.write(ridA + "%" + ridB +"\n");
		 		            String a = ridA + "%" + ridB;
		 		            real += a+ spe + ":";
		 		            
		 		            if(!result.contains(a)){
		 		            	System.out.println(rec1.toString());
		 		            	System.out.println(rec2.toString());
		 		            	
	
								System.out.println(++count);
					            	
		 		            	System.out.println(rec1.getTextual() + "------" + rec2.getTextual());
		 		            	System.out.println();
		 		            }
		 		            
		 		        }
		    			
		    		}
		            
		    	}
		    	
		    }
		    
		}
		
		int count2 = 0;
//		System.out.println(real);
//		real = "2244019654%518637278" + spe;
		
		try{
			reader = new BufferedReader(new FileReader(file));
	
			
			
			String tempString = null;
			
			while((tempString = reader.readLine()) != null){
				String a = tempString;
				
				if(!real.contains(a)){
					System.out.println("start");
					System.out.println(++count);
					System.out.println(tempString);
				}
//				System.out.println();
			}
		}catch(Exception e){
			
		}
		
		writer.close();
		System.out.println(firstCount);
		System.out.println(SecondCount);
		System.out.println(ThirdCount);
		System.out.println((double)firstCount/rRecords.size()/sRecords.size());
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
