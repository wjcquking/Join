package org.macau.local.analysis.time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.macau.flickr.temporal.TemporalUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class SpatialTime {

	public static double getTokenSimilarity(String iToken,String jToken){
		//System.out.println("iToken:" + iToken + "jToken:" + jToken);
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(";")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(";")));
		
		int i_num = itext.size();
		int j_num = jtext.size();
//		System.out.println(i_num + " " + j_num);
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
	
	public static void main(String[] args){
		ArrayList<FlickrData> records = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.dataPath);
		float similarityThreshold = 0.5F;
		
		Map<Long,ArrayList<FlickrData>> map = new HashMap<Long,ArrayList<FlickrData>>();
		
//		for(FlickrData record: records){
//			
//			long time = record.getTimestamp() / TemporalUtil.MS_OF_ONE_DAY;
//			if(map.get(time) == null){
//				ArrayList<FlickrData> datas =  new ArrayList<FlickrData>();
//				datas.add(record);
//				map.put(time, datas);
//			}else{
//				map.get(time).add(record);
//			}
//			
//		}
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
		for (int i = 0; i < 5000; i++) {
			FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < 5000; j++) {
		    	FlickrData rec2 = records.get(j);
		    	
		    		if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    	
		    		firstCount++;
		    		if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		
		    		
		    			SecondCount++;
		    				
		    			if (getTokenSimilarity(rec1.getTextual(), rec2.getTextual()) >= similarityThreshold) {
		 		        	ThirdCount++;
		 		            long ridA = rec1.getId();
		 		            long ridB = rec2.getId();
		 		            if (ridA < ridB) {
		 		                long rid = ridA;
		 		                ridA = ridB;
		 		                ridB = rid;
		 		            }
		 		            
		 		        }
		    			
		    		}
		            
		    	}
		    	
		    }
		    
		}
		System.out.println(firstCount);
		System.out.println(SecondCount);
		System.out.println(ThirdCount);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
