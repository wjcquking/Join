package org.macau.local.file;


import java.util.ArrayList;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.spatial.Distance;

public class SpatialFirst {
	
	public static final double DISTANCE_THRESHOLD = 0.001;
	
	public static void main(String[] args){
		
		ArrayList<FlickrData> records = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.dataPath);
		
		int size = 10;
		Long startTime = System.currentTimeMillis();
		int compareCount = 0;
		int count = 0;
		int count2 = 0;
		for (int i = 0; i < size; i++) {
			FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < size; j++) {
		    	compareCount++;
		    	
		    	FlickrData rec2 = records.get(j);
		    	long ridA = rec1.getId();
	            long ridB = rec2.getId();
	            
	            System.out.println(i + ":" + j + ":" + (float)Math.abs(rec1.getTimestamp()- rec2.getTimestamp())/(24*3600*1000));
	            System.out.println(70*24*3600*1000 + ":"+Distance.GreatCircleDistance(rec1.getLat(), rec1.getLon(), rec2.getLat(), rec2.getLon()) + ":" + DISTANCE_THRESHOLD + ":" + (Distance.GreatCircleDistance(rec1.getLat(), rec1.getLon(), rec2.getLat(), rec2.getLon()) < DISTANCE_THRESHOLD));
	            
		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		System.out.println(count);
		            count++;
		    	}
		    	if(Distance.GreatCircleDistance(rec1.getLat(), rec1.getLon(), rec2.getLat(), rec2.getLon()) < DISTANCE_THRESHOLD){
		    		System.out.println(count2);
		    		count2++;
		    	}
		    	
		    }
		}
		System.out.println(compareCount +":" + size*(size-1)/2);
		System.out.println(count + ":" + (double)count/compareCount);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
