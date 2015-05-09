package org.macau.stjoin.basic.temporal.combination;

/**************************************************
 * 
 * The mapper uses the temporal and spatial information
 * 
 * For the Temporal information
 * R send one time interval(one replication)
 * S send one more time interval
 * 
 * For the Spatial Information
 * R send the Z order
 * S send all the covered cell z order
 * 
 *************************************************/

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.basic.spatial.GridSpatialMapper;

public class TemporalSpatialCombinationJoinMapper extends
	Mapper<Object, Text, Text, FlickrValue>{
	
	private final Text outputKey = new Text();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal mapper Start at " + System.currentTimeMillis());
	}
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
//		outputValue = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
		outputValue.setTiles(value.toString().split(":")[5]);
		
		outputValue.setTimestamp(timestamp);
		
		
		/***************************
		 * 
		 * The Original temporal partition, for each time interval, it is a partition, for the R
		 * the time interval is the key, while for the S set, it should set to three time interval
		 * 
		 ***************************/
		
		List<Long> rList = new ArrayList<Long>();
		List<Long> sList = new ArrayList<Long>();
		
		/********************************************
		 * Choose some part to partition other feature. The Choosing method is different
		 * 
		 * In the following, we use some simple method, First analyze the data, find the part whose weight is the highest
		 * For example the time interval is 206, so for the R set we put the 206-207, while for the S set, we put the 205-208
		 * 
		********************************************/
		long point = 206L;
		
		for(long i = point -1;i <= point + 1;i++){
			rList.add(i);
		}
		
		for(long i = point -2; i <= point +2;i++){
			sList.add(i);
		}
		
		ArrayList<Integer> tileList = new ArrayList<Integer>();
		
		/***********************************
		 * 
		 * I want to divide the data into two part and see the result 
		 * improve the result
		 * I choose one time interval that has the maximum as the point
		 * Choose three time interval in R
		 * Choose Five time interval in S
		 * 
		 * For the Key-Value
		 * Key: add some Feature String Tag, For example, S is for Spatial, T is for the Temporal
		 * Value: The record
		 * 
		 **********************************/
		
		if(tag == FlickrSimilarityUtil.S_tag){
			
			if(sList.contains(timeInterval)){
				
				tileList = GridSpatialMapper.tileOfS(lat,lon);
				
				if(timeInterval != point){
					
					for(int i = -1; i<=1;i++){
						long keyValue = timeInterval + i;
						outputValue.setTileNumber((int)timeInterval + i);
						outputKey.set(FlickrSimilarityUtil.Temporal_TAG +keyValue);
						context.write(outputKey, outputValue);
					}
				}
				
				for(Integer tile: tileList){
					
					outputValue.setTileNumber(tile);
//					outputKey.set("S:"+GridPartition.paritionNumber(tile));
					outputKey.set(FlickrSimilarityUtil.Spatial_TAG +tile);
					context.write(outputKey, outputValue);
					
					
				}
				
				
				
			}else{
				
				for(int i = -1; i<=1;i++){
					long keyValue = timeInterval + i;
					outputValue.setTileNumber((int)timeInterval + i);
					outputKey.set(FlickrSimilarityUtil.Temporal_TAG +keyValue);
					context.write(outputKey, outputValue);
				}
				
			}
		}else{
			
			//for the R set
			if(sList.contains(timeInterval)){
				
				tileList = GridSpatialMapper.tileNumberOfR(lat,lon);
				
				for(Integer tile: tileList){
					
					outputValue.setTileNumber(tile);
					outputKey.set(FlickrSimilarityUtil.Spatial_TAG +tile);
					context.write(outputKey, outputValue);
				}
				
			}else{
				
				outputValue.setTileNumber((int)timeInterval);
				outputKey.set(FlickrSimilarityUtil.Temporal_TAG + timeInterval);
				context.write(outputKey, outputValue);
				
			}
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n");
		
	}
}