package org.macau.flickr.spatial.basic;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class BasicSpatialReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		for(FlickrValue value:values){
			FlickrValue recCopy = new FlickrValue(value);
		    records.add(recCopy);
		}
		
		//System.out.println("key is " + key + "record size is " + records.size());
		
		for (int i = 0; i < records.size(); i++) {
			FlickrValue rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrValue rec2 = records.get(j);
/*		    	System.out.println(rec1);
		    	System.out.println(rec2);*/
//		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
//		    		continue;
//		    	}
		    	
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
		            context.write(text, new Text(""));
		    	}
		    	
		    }
		}
		records.clear();
	}
	
}
