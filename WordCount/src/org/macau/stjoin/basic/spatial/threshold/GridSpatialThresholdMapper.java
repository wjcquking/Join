package org.macau.stjoin.basic.spatial.threshold;

/**
 * @author: wangjian
 * @date: 2013-11-12
 * Last modify date: 2014-10-21
 * Last Modify date: 2015-05-16
 * 
 * 
 * the idea comes from the paper "SJMR:Parallelizing Spatial Join with MapReduce",IEEE, 2009
 * 
 * The MapFunction is for R join S
 * The solution description:
 * Partition R in one way grid and Partition S in the extension grid, which means that one grid in R just need to 
 * compare one grid in the S, but there may be too many replications in S if the distance threshold is large.
 * 
 * 
 * 
 * add some change to see how to work with subversion in the eclipse
 */



import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author mb25428
 * Read the flickr data, then extract the spatial information, send the object to other machine
 * For Example
 * The Data form:ID;locationID, lat;lon; timestamp, textual 
 * The Data Example:1093113743:215929:48.89899:2.380696:973929974000:0;82;525;2479;19649;25431;31250;51203
 * 
 * Using Threshold value as the key
 * 
 * 
 */


public class GridSpatialThresholdMapper extends
Mapper<Object, Text, Text, FlickrValue>{


	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Mapper setup" + System.currentTimeMillis());
	}
	
	
	private Text outputKey = new Text();
	private final FlickrValue outputValue = new FlickrValue();
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
		
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		

		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		

		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		outputValue.setTimestamp(timestamp);
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int x = (int) (lat /thres);
		int y = (int)(lon/thres );
		
		
		
		outputValue.setTiles(value.toString().split(":")[5]);
		outputValue.setOthers(value.toString().split(":")[6]);
		
		/*
		 * for R, there is only need one tile
		 * but for S, the data should send to other tiles
		 */
		String textual = value.toString().split(":")[5];
		
		if(!textual.equals("null")){
			if(tag == FlickrSimilarityUtil.R_tag){
				outputValue.setTileNumber(0);
				outputKey.set("x:" + x + ":y:" + y);
				context.write(outputKey, outputValue);
				
			}else{
				for(int i = x-1; i <= x+1;i++){
					for(int j = y-1;j <= y+1;j++){
						outputValue.setTileNumber(0);
						outputKey.set("x:" + i + ":y:" + j);
						context.write(outputKey, outputValue);
					}
				}
			}
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Mapper end at " + System.currentTimeMillis() + "\n");
		
	}
	
	
}
