package org.macau.stjoin.basic.temporal.mixture;

/**************************************************
 * @author wangjian
 * The mapper uses the temporal and spatial information
 * 
 * For the different data set
 * R send one time interval(one replication)
 * S send one more time interval 
 * 
 * 
 * Date: 2014-11-19
 * 
 *************************************************/
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.basic.spatial.GridSpatialMapper;

public class TemporalSpatialMixtureJoinMapper extends
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
		
		
		
		ArrayList<Integer> tileList = new ArrayList<Integer>();
		
		/***********************************
		 * 
		 * Assign the key of the record with two features
		 * The Replication is larger
		 * 
		 * For the Key-Value
		 * Key: add some Feature String Tag
		 * Value: The record
		 * 
		 **********************************/
		
		if(tag == FlickrSimilarityUtil.S_tag){
			
				
			tileList = GridSpatialMapper.tileOfS(lat,lon);
			
			for(Integer tile: tileList){
				
				outputValue.setTileNumber(tile);
				
				for(int i = -1; i<=1;i++){
					long keyValue = timeInterval + i;
					outputKey.set(FlickrSimilarityUtil.Spatial_TAG +tile + FlickrSimilarityUtil.Temporal_TAG +keyValue);
					context.write(outputKey, outputValue);
				}
			}
				
		}else{
			
			//for the R set
			
			tileList = GridSpatialMapper.tileNumberOfR(lat,lon);
			
			for(Integer tile: tileList){
				
				outputValue.setTileNumber(tile);
				outputKey.set(FlickrSimilarityUtil.Spatial_TAG +tile + FlickrSimilarityUtil.Temporal_TAG + timeInterval);
				context.write(outputKey, outputValue);
			}
			
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n");
	}
}