package org.macau.stjoin.mapper.improved.phase2;

/**
 * The Mapper uses the temporal information
 * 
 * 
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.util.DataSimilarityUtil;

public class MapperSideJoinMapper extends
	Mapper<Object, Text, LongWritable, FlickrValue>{
	
	
	private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
	private final Map<Integer,ArrayList<FlickrValue>> sMap = new HashMap<Integer,ArrayList<FlickrValue>>();
	
	
	
	private int startTimeInterval = 100000;
	private int endTimeInterval = 0;
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("The mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		FlickrValue fv = new FlickrValue();
		
		
		//550	0   37842881:87148:48.858188:2.29449:665337600000:0;8;13:AAAAAAAAAA
		
		int timeinterval = Integer.parseInt(value.toString().split("\\s+")[0]);
		
		
		fv.setTileNumber(timeinterval);
		fv.setTag(Integer.parseInt(value.toString().split("\\s+")[1]));
		
		if(timeinterval < startTimeInterval){
			startTimeInterval = timeinterval;
		}
		
		if(timeinterval > endTimeInterval ){
			endTimeInterval = timeinterval;
		}
		
		String record = value.toString().split("\\s+")[2];
		
		DataSimilarityUtil.getFlickrValue(fv, record);

		
		if(!fv.getTiles().equals("null")){
			
			if(fv.getTag() == FlickrSimilarityUtil.R_tag){
				
				for(int i = fv.getTileNumber()-1;i <= fv.getTileNumber()+1;i++){
					if(sMap.containsKey(i)){
						for(int k = 0; k < sMap.get(i).size();k++){
							FlickrValue value2 = sMap.get(i).get(k);
							if(FlickrSimilarityUtil.TemporalSimilarity(fv, value2)){
								if(FlickrSimilarityUtil.SpatialSimilarity(fv, value2)){
									
									if(FlickrSimilarityUtil.TextualSimilarity(fv, value2)){
										
										
										long ridA = fv.getId();
							            long ridB = value2.getId();
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
									
						            
							            value2.setOthers("" + ridA + "%" + ridB);
							            context.write(new LongWritable(0), value2);
									}
								}
							}
						}
					}
				}
				
		    	
		    	if(rMap.containsKey(fv.getTileNumber())){
			    	
			    	rMap.get(fv.getTileNumber()).add(new FlickrValue(fv));
			    	
			    }else{
			    	
			    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
			    	
			    	list.add(fv);
			    	
			    	rMap.put(fv.getTileNumber(), list);
			    	
			    }
		    }else{
		    	
		    	for(int i = fv.getTileNumber()-1;i <= fv.getTileNumber()+1;i++){
					if(rMap.containsKey(i)){
						for(int k = 0; k < rMap.get(i).size();k++){
							FlickrValue value2 = rMap.get(i).get(k);
							if(FlickrSimilarityUtil.TemporalSimilarity(fv, value2)){
								if(FlickrSimilarityUtil.SpatialSimilarity(fv, value2)){
									
									if(FlickrSimilarityUtil.TextualSimilarity(fv, value2)){
										
										
										long ridA = fv.getId();
							            long ridB = value2.getId();
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
									
						            
							            value2.setOthers("" + ridA + "%" + ridB);
							            context.write(new LongWritable(0), value2);
									}
								}
							}
						}
					}
				}
		    	
		    	
		    	if(sMap.containsKey(fv.getTileNumber())){
			    	
			    	sMap.get(fv.getTileNumber()).add(new FlickrValue(fv));
			    	
			    }else{
			    	
			    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
			    	
			    	list.add(fv);
			    	
			    	sMap.put(fv.getTileNumber(), list);
			    }
		    }
		}
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
//		System.out.println("A" + startTimeInterval);
//		System.out.println("B" + endTimeInterval);
		
		if(rMap.containsKey(startTimeInterval)){
			for(int k = 0; k < rMap.get(startTimeInterval).size();k++){
				FlickrValue value2 = rMap.get(startTimeInterval).get(k);
				context.write(new LongWritable(value2.getTileNumber()), value2);
				
			}
		}
		
		if(rMap.containsKey(endTimeInterval)){
			for(int k = 0; k < rMap.get(endTimeInterval).size();k++){
				FlickrValue value2 = rMap.get(endTimeInterval).get(k);
//				context.write(new LongWritable(value2.getTileNumber()), value2);
				context.write(new LongWritable(value2.getTileNumber()+1), value2);
				
			}
		}
		
		if(sMap.containsKey(startTimeInterval)){
			for(int k = 0; k < sMap.get(startTimeInterval).size();k++){
				FlickrValue value2 = sMap.get(startTimeInterval).get(k);
				context.write(new LongWritable(value2.getTileNumber()), value2);
				
			}
		}
		
		if(sMap.containsKey(endTimeInterval)){
			for(int k = 0; k < sMap.get(endTimeInterval).size();k++){
				FlickrValue value2 = sMap.get(endTimeInterval).get(k);
//				context.write(new LongWritable(value2.getTileNumber()), value2);
				context.write(new LongWritable(value2.getTileNumber()+1), value2);
				
			}
		}
		rMap.clear();
		sMap.clear();
		System.out.println("The mapper end at " + System.currentTimeMillis() + "\n");
	}
}