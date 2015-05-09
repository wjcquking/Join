package org.macau.flickr.spatial.grid;

/**
 * There are two step
 * Filter Step: 
 * Input: the tuples belongs to the same partitions
 * the goal is to find the paris of intersecting rectangles between the two sets by strip-based plane sweeping techniques.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class GridSpatialReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
	private final Map<Integer,ArrayList<FlickrValue>> sMap = new HashMap<Integer,ArrayList<FlickrValue>>();

	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		
		System.out.println(key);
		
		for(FlickrValue value:values){
			
			/*
			 * We need new a FlickrValue, if not, the values in Map.getTag() will become the same
			 * this may because the address of the value is the same, when change a value, all the values
			 * in the Map.getTag will become the same
			 * 
			 */
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
			@SuppressWarnings("unchecked")
			Map.Entry<Integer,ArrayList<FlickrValue>> m = (Map.Entry<Integer,ArrayList<FlickrValue>>)it.next();
			m.getKey();
			ArrayList<FlickrValue> rRecords = m.getValue();
			//System.out.println(sMap.get(m.getKey()));
			ArrayList<FlickrValue> sRecords = new ArrayList<FlickrValue>();
			
			/*
			 * This is important,because the sMap may don't have the key value
			 * so add one if condition to make sure the sMap can get some value by the key
			 */
			if(sMap.get(m.getKey()) != null){
				sRecords = sMap.get(m.getKey());
			}
			
			
			System.out.println(rRecords.size() + " r" );
			System.out.println(sRecords.size() + " s" );
			
			
//			brute force
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
		}
		
	}
	
}
