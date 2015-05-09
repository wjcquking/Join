package org.macau.flickr.temporal;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author hadoop
 * for each 
 */

public class TemporalBasicReducer extends
	Reducer<LongWritable, FlickrValue, Text, Text>{
	
	
	private final Text text = new Text();
	private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	private final ArrayList<FlickrValue> aRecords = new ArrayList<FlickrValue>();
	private final ArrayList<FlickrValue> bRecords = new ArrayList<FlickrValue>();
	
	public void reduce(LongWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		for(FlickrValue value:values){
			FlickrValue recCopy = new FlickrValue(value);
			if(recCopy.getTag() == 0){
				aRecords.add(recCopy);
		    }else{
		    	bRecords.add(recCopy);
		    }
		}
		
		//n^2
		/*
		for (int i = 0; i < records.size(); i++) {
			FlickrValue rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrValue rec2 = records.get(j);
		    	
		    	//if satisfy the spatial condition, go on
		    	//if not, stop and go to next loop
		    	if(!FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		continue;
		    	}
		    	
		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    		
		    		long ridA = rec1.getId();
		            long ridB = rec2.getId();
		            if (ridA < ridB) {
		                long rid = ridA;
		                ridA = ridB;
		                ridB = rid;
		            }
		            
		            text.set("" + ridA + "%" + ridB);
		            context.write(text, new Text(""));
		    	}
		    }
		}
		
		records.clear();
		*/
		
		//add a tag for the record
		//in the same tag, they don't need to be compared for the temporal
		//build local index for every time period data.
		for (int i = 0; i < aRecords.size(); i++) {
			FlickrValue rec1 = aRecords.get(i);
		    for (int j = i + 1; j < aRecords.size(); j++) {
		    	FlickrValue rec2 = aRecords.get(j);
		    	
		    	//if satisfy the spatial condition, go on
		    	//if not, stop and go to next loop
		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		long ridA = rec1.getId();
		            long ridB = rec2.getId();
		            if (ridA < ridB) {
		                long rid = ridA;
		                ridA = ridB;
		                ridB = rid;
		            }
		            text.set("" + ridA + "%" + ridB);
		            System.out.println(rec1.getId() + "%" + rec2.getId() + "%" + FlickrSimilarityUtil.SpatialDistance(rec1, rec2));
//		            System.out.println("A" + ridA + "%" + ridB);
		            context.write(new Text(rec1.getId() + "%" + rec2.getId()), new Text("" + FlickrSimilarityUtil.SpatialDistance(rec1, rec2)));
		    	}
		    }
		    
		    for(int k = 0; k < bRecords.size();k++){
		    	FlickrValue rec3 = bRecords.get(k);
		    	
		    	if(!FlickrSimilarityUtil.SpatialSimilarity(rec1, rec3)){
		    		continue;
		    	}
		    	
		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec3)){
		    		
		    		long ridA = rec1.getId();
		            long ridB = rec3.getId();
		            if (ridA < ridB) {
		                long rid = ridA;
		                ridA = ridB;
		                ridB = rid;
		            }
		            
		            text.set("" + ridA + "%" + ridB);
//		            System.out.println("B" + ridA + "%" + ridB);
//		            context.write(text, new Text(""));
		            context.write(new Text(rec1.getId() + "%" + rec3.getId()), new Text("" + FlickrSimilarityUtil.SpatialDistance(rec1, rec3)));
		    	}
		    	
		    }
		}
		
		aRecords.clear();
		bRecords.clear();
		
		
		
		//build local index by using the spatial information to get the similar objects
		
		
		//
	}
}
