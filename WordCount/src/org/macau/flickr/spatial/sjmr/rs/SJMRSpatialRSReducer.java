package org.macau.flickr.spatial.sjmr.rs;

/**
 * There are two step
 * Filter Step: 
 * Input: the tuples belongs to the same partitions
 * the goal is to find the paris of intersecting rectangles between the two sets by strip-based plane sweeping techniques.
 * because each tile in R just need to compare one tile in the S, so use the tile-based sweeping techniques is good choice.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SJMRSpatialRSReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
	private final Map<Integer,ArrayList<FlickrValue>> sMap = new HashMap<Integer,ArrayList<FlickrValue>>();

	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		
		int count = 0;
		System.out.println(key);
//		System.out.println(values.toString());
		
		for(FlickrValue value:values){
//			System.out.println(value);
//			System.out.println("tile " + value.getTileNumber());
//			System.out.println(count++);
			/*We need new a FlickrValue, if not, the values in Map.getTag() will become the same
			 * this may because the address of the value is the same, when change a value, all the values
			 * in the Map.getTag will become the same
			 * 
			 * */
			FlickrValue fv = new FlickrValue(value);
			
			//R
			if(fv.getTag() == 0){
				if(rMap.containsKey(value.getTileNumber())){
					
					rMap.get(value.getTileNumber()).add(fv);

				}else{

					ArrayList<FlickrValue> recordList = new ArrayList<FlickrValue>();
					recordList.add(fv);
					rMap.put(new Integer(value.getTileNumber()),recordList);
					
				}
			}else{
				if(sMap.containsKey(value.getTileNumber())){
					
					sMap.get(value.getTileNumber()).add(fv);

				}else{

					ArrayList<FlickrValue> recordList = new ArrayList<FlickrValue>();
					recordList.add(fv);
					sMap.put(new Integer(value.getTileNumber()),recordList);
					
				}
			}
		}
		

		
		Iterator it = rMap.entrySet().iterator();
		
		
		while(it.hasNext()){
			Map.Entry<Integer,ArrayList<FlickrValue>> m = (Map.Entry<Integer,ArrayList<FlickrValue>>)it.next();
			m.getKey();
			ArrayList<FlickrValue> rRecords = m.getValue();
			//System.out.println(sMap.get(m.getKey()));
			ArrayList<FlickrValue> sRecords = new ArrayList<FlickrValue>();
			
			/*
			 * This is important,because the sMap may don't have the key value
			 * 
			 */
			if(sMap.get(m.getKey()) != null){
				sRecords = sMap.get(m.getKey());
			}
			
			
			System.out.println(rRecords.size() + " r" );
			System.out.println(sRecords.size() + " s" );
			
			
//			brute force
			for (int i = 0; i < rRecords.size(); i++) {
				FlickrValue rec1 = rRecords.get(i);
				
				//System.out.println(rec1);
			    for (int j = 0; j < sRecords.size(); j++) {
			    	
			    	
			    	FlickrValue rec2 = sRecords.get(j);
			    	//System.out.println(rec2);
			    	
			    	long ridA = rec1.getId();
		            long ridB = rec2.getId();
			    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
			    		
//			            if (ridA < ridB) {
//			                long rid = ridA;
//			                ridA = ridB;
//			                ridB = rid;
//			            }

			            text.set(ridA + "%" + ridB);
			            context.write(text, new Text(""));
			    	}
			    	
			    }
			}
		}
		
	}
	
}
