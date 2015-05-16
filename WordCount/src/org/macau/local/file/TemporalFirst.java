package org.macau.local.file;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class TemporalFirst {
	
	
	/**
	 * TSO: Temporal Spatial Textual
	 */
	public static void main(String[] args) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);

		
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
		 		        }
		    			
		    		}
		            
		    	}
		    	
		    }
		    
		}
		writer.close();
		System.out.println(firstCount);
		System.out.println(SecondCount);
		System.out.println(ThirdCount);
		System.out.println((double)firstCount/rRecords.size()/sRecords.size());
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
