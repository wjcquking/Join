package org.macau.flickr.spatial.basic;

/**
 * There are two step
 * Filter Step: 
 * Input: the tuples belongs to the same partitions
 * the goal is to find the paris of intersecting rectangles between the two sets by strip-based plane sweeping techniques.
 * because each tile in R just need to compare one tile in the S, so use the tile-based sweeping techniques is good choice.
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class BasicSpatialRSReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final ArrayList<FlickrValue> rRecords = new ArrayList<FlickrValue>();
	private final ArrayList<FlickrValue> sRecords = new ArrayList<FlickrValue>();
	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		
		for(FlickrValue value:values){

			FlickrValue fv = new FlickrValue(value);
			
			//R
			if(fv.getTag() == 0){
				rRecords.add(fv);
			}else{
				sRecords.add(fv);
			}
		}

					
//		brute force
		for (int i = 0; i < rRecords.size(); i++) {
			FlickrValue rec1 = rRecords.get(i);
			
		    for (int j = 0; j < sRecords.size(); j++) {
		    	
		    	FlickrValue rec2 = sRecords.get(j);
		    	
		    	long ridA = rec1.getId();
	            long ridB = rec2.getId();
		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){

		            text.set(ridA + "%" + ridB);
		            context.write(text, new Text(""));
		    	}
		    	
		    }
		}
		//this is important, clear the records, to store the data
		rRecords.clear();
		sRecords.clear();
	}
	
}
