package org.macau.flickr.temporal;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TemporalBasicReducerTest {

	public static void main(String[] args){
		
		final Text text = new Text();
		final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
		
		records.add(new FlickrValue(1093113743,48.89899,2.380696,1004167107000L));
		records.add(new FlickrValue(87781949,48.89061,2.354716,1004167307000L));
		
		for (int i = 0; i < records.size(); i++) {
			FlickrValue rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrValue rec2 = records.get(j);
		    	
		    	System.out.println(rec1);
		    	System.out.println(rec2);
		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    		System.out.println(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2));
		    		continue;
		    	}
		    	
		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		long ridA = rec1.getId();
		            long ridB = rec2.getId();
		            if (ridA < ridB) {
		                long rid = ridA;
		                ridA = ridB;
		                ridB = rid;
		            }
		            System.out.println(ridA + "%" + ridB);
		            text.set(ridA + "%" + ridB);
		    	}
		    	
		    }
		}
	}
}
