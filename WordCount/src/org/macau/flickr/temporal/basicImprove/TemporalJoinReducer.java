package org.macau.flickr.temporal.basicImprove;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TemporalJoinReducer extends
	Reducer<LongWritable, FlickrValue, LongWritable, FlickrValue>{
		
		
		private final Text text = new Text();
		private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
		private final long MS_OF_ONE_DAY = 86400000;
		
		
		public void reduce(LongWritable key, Iterable<FlickrValue> values,
				Context context) throws IOException, InterruptedException{
			for(FlickrValue value:values){
				FlickrValue recCopy = new FlickrValue(value);
			    //records.add(recCopy);
			    context.write(key, recCopy);
			}
			
			//System.out.println("key is " + key + "record size is " + records.size());
			
//			for (int i = 0; i < records.size(); i++) {
//				FlickrValue rec1 = records.get(i);
//			    for (int j = i + 1; j < records.size(); j++) {
//			    	FlickrValue rec2 = records.get(j);
//			    	
//			    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
//			    		
//			    		long ridA = rec1.getId();
//			            long ridB = rec2.getId();
//			            if (ridA < ridB) {
//			                long rid = ridA;
//			                ridA = ridB;
//			                ridB = rid;
//			            }
//			            //System.out.println("" + ridA + "%" + ridB);
//			            
//			            text.set("" + ridA + "%" + ridB);
//			            context.write(text, new Text(""));
//			    	}
//			    }
//			}
//			
//			records.clear();
		}
	}

