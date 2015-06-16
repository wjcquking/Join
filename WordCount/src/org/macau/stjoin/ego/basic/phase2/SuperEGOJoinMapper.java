package org.macau.stjoin.ego.basic.phase2;

/****************************************************
 * 
 * @author hadoop
 * Map: Input:  KEY  : 
 *              Value:
 *      output: KEY  :
 *      		Value:
 *      
 *      It use the 
 * 		It use some threshold to get the proper value 
 * 		so I need some statistics
 * 
 * Date: 2014-12-29
 ****************************************************/
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SuperEGOJoinMapper extends
	Mapper<Object, Text, Text, FlickrValue>{
	
	private final Text outputKey = new Text();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("SuperEGO mapper Start at " + System.currentTimeMillis());
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//the data  example-----0 837	0 837 1314836729:181792:48.874397:2.294511:1013548725000:0:AAAA
		String[] values = value.toString().split("\\s+");
		
		String record = values[4];
		
		int group = Integer.parseInt(values[0]);
		int tag = Integer.parseInt(values[2]);
		
		long id =Long.parseLong(record.toString().split(":")[0]);
		double lat = Double.parseDouble(record.toString().split(":")[2]);
		double lon = Double.parseDouble(record.toString().split(":")[3]);
		long timestamp = Long.parseLong(record.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
		outputValue.setTiles(record.toString().split(":")[5]);
		outputValue.setOthers(record.toString().split(":")[6]);
		String textual = value.toString().split(":")[5];
		
//		outputValue.setTiles(record.toString().split(":")[4]);
//		outputValue.setOthers(record.toString().split(":")[5]);
//		String textual = value.toString().split(":")[4];
		
		outputValue.setTimestamp(timestamp);
		
		String rPartition = "1:1,2:1,2:2,3:3,4:4,5,6:5,6,7:7,8:8,9:9,10";
		String sPartition = "1,2,3:2,3:4,5:5,6:6,7:6,7:7,8:8,9:9,10:10";
		
		
		if(!textual.equals("null")){
			if(tag == FlickrSimilarityUtil.S_tag){
				
				for(String str : sPartition.split(":")[group].split(",")){
					int i = Integer.parseInt(str)-1;
					outputKey.set(group + " " + i);
					outputValue.setTileNumber((int)timeInterval);
					context.write(outputKey, outputValue);
				}
//				for(int i = 0; i < 10;i++){
//					outputKey.set(group + " " + i);
//					outputValue.setTileNumber((int)timeInterval);
//					context.write(outputKey, outputValue);
//				}
			}else if(tag == FlickrSimilarityUtil.R_tag){
				for(String str : rPartition.split(":")[group].split(",")){
					int i = Integer.parseInt(str)-1;
					outputKey.set(i+ " " + group) ;
					outputValue.setTileNumber((int)timeInterval);
					context.write(outputKey, outputValue);
				}
				
//				for(int i = group-1; i <= group;i++){
//					outputKey.set(i + " " + group) ;
//					outputValue.setTileNumber((int)timeInterval);
//					context.write(outputKey, outputValue);
//				}
				
			}
		}
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The SuperEGO mapper end at " + System.currentTimeMillis() + "\n" );
	}
}