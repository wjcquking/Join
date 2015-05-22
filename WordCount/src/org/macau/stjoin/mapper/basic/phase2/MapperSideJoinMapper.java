package org.macau.stjoin.mapper.basic.phase2;

/**
 * The Mapper uses the temporal information
 * 
 * The same key in one file
 * each Mapper  read one file
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class MapperSideJoinMapper extends
	Mapper<Object, Text, Text, Text>{
	
	private final Text text = new Text();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("The mapper Start at " + System.currentTimeMillis());
	}
	
	private final List<FlickrValue> rList = new ArrayList<FlickrValue>();
	private final List<FlickrValue> sList = new ArrayList<FlickrValue>();
	
	private int currentTimeInterval = 0;
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		FlickrValue fv = new FlickrValue();
		
		
		//550	0:37842881:87148:48.858188:2.29449:665337600000:0;8;13:AAAAAAAAAA
		
		int timeinterval = Integer.parseInt(value.toString().split("\\s+")[0]);
		
		if(currentTimeInterval == 0){
			currentTimeInterval = timeinterval;
		}
		
		String record = value.toString().split("\\s+")[1];
		
		String[] recordArray = record.split(":");
		
		fv.setTag(Integer.parseInt(recordArray[0]));
		fv.setId(Long.parseLong(recordArray[1]));
		fv.setLat(Double.parseDouble(recordArray[3]));
		fv.setLon(Double.parseDouble(recordArray[4]));
		fv.setTimestamp(Long.parseLong(recordArray[5]));
		fv.setTiles(record.split(":")[6]);
		
		
		
		if(!fv.getTiles().equals("null")){
			
			if(timeinterval == currentTimeInterval){
			
				if(fv.getTag() == FlickrSimilarityUtil.R_tag){
					if(!sList.isEmpty()){
						for(FlickrValue fValue : sList){
							if(FlickrSimilarityUtil.TemporalSimilarity(fv, fValue)){
								if(FlickrSimilarityUtil.SpatialSimilarity(fv, fValue)){
									
									if(FlickrSimilarityUtil.TextualSimilarity(fv, fValue)){
										
										long ridA = fv.getId();
							            long ridB = fValue.getId();
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
						}
					}
					rList.add(fv);
				}else{
					if(!rList.isEmpty()){
						for(FlickrValue fValue : rList){
							if(FlickrSimilarityUtil.TemporalSimilarity(fv, fValue)){
								if(FlickrSimilarityUtil.SpatialSimilarity(fv, fValue)){
									
									if(FlickrSimilarityUtil.TextualSimilarity(fv, fValue)){
										
										long ridA = fv.getId();
							            long ridB = fValue.getId();
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
						}
					}
					sList.add(fv);
				}
			}else{
				rList.clear();
				sList.clear();
				if(fv.getTag() == FlickrSimilarityUtil.R_tag){
					rList.add(fv);
				}else{
					sList.add(fv);
				}
				currentTimeInterval = timeinterval;
			}
		}
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The mapper end at " + System.currentTimeMillis() + "\n");
	}
}