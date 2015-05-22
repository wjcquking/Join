package org.macau.stjoin.mapper.improved.egophase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrValue;

public class EGOOptimalJoinReducer extends
	Reducer<LongWritable, FlickrValue, Text, Text>{
		
		
		private final Text text = new Text();
		
		private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		

		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("The reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(LongWritable key, Iterable<FlickrValue> values,
				Context context) throws IOException, InterruptedException{
			int min = 100000; 
			int max = 0;
			
			for(FlickrValue value:values){
				
				FlickrValue fv = new FlickrValue(value);
				if(value.getTileNumber() < min){
					min = value.getTileNumber();
				}
				
				if(value.getTileNumber() > max){
					max = value.getTileNumber();
				}
				
				if(rMap.containsKey(value.getTileNumber())){
			    	
			    	rMap.get(value.getTileNumber()).add(new FlickrValue(fv));
			    	
			    }else{
			    	
			    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
			    	
			    	list.add(fv);
			    	
			    	rMap.put(value.getTileNumber(), list);
			    	
			    }
				
			    
			}
		
			for(int i = min; i <= max;i++){
				if(rMap.containsKey(i)){
					for(int j = 0;j < rMap.get(i).size();j++){
						FlickrValue value1 = rMap.get(i).get(j);
						
						text.set(value1.getTileNumber() + "    " + value1.getTag());
						context.write(text, new Text(value1.getOthers()));
					}
				}
			}
			
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			System.out.println("clean up");
			System.out.println("The Reducer End at"+System.currentTimeMillis());
			
		}
	}

