package org.macau.stjoin.basic.spatial;

/**
 * @author: wangjian
 * @date: 2013-11-12
 * Last modify date: 2014-10-21
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
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.flickr.spatial.partition.*;
import org.macau.spatial.Distance;

/**
 * 
 * @author mb25428
 * Read the flickr data, then extract the spatial information, send the object to other machine
 * For Example
 * The Data form:ID;locationID, lat;lon; timestamp, textual 
 * The Data Example:1093113743:215929:48.89899:2.380696:973929974000:0;82;525;2479;19649;25431;31250;51203
 * 
 * 
 * 
 * 
 */


public class GridSpatialMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	/*
	 * use the z curve order and round-robin algorithm to find the best partition function
	 * the universe is divided regularly into Nt tiles
	 * each tile is number from 0 to Nt-1 according to z curve, 
	 * and mapped to a partition p with a round robin scheme
	 * 
	 */

	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Grid Spatial Mapper setup");
	}
	
	public static ArrayList<Integer> tileNumberOfR(double lat,double lon){
		
		ArrayList<Integer> list = new ArrayList<Integer>();
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/(FlickrSimilarityUtil.wholeSpaceWidth / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE));
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/(FlickrSimilarityUtil.WholeSpaceLength / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE));
		
		list.add(ZOrderValue.parseToZOrder(latNumber, lonNumber));
		return list;
		
	}
	
	public static ArrayList<Integer> tileOfS(double lat, double lon){
		ArrayList<Integer> list = new ArrayList<Integer>();
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int latNumberStart = (int) ((lat - thres - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		if(latNumberStart < 0){
			latNumberStart = 0;
		}
		int latNumberEnd = (int) ((lat + thres - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumberStart =(int)((lon-thres- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		if(lonNumberStart < 0){
			lonNumberStart = 0;
		}
		int lonNumberEnd = (int)((lon + thres - FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		
		for(int i = latNumberStart;i <= latNumberEnd;i++){
			for(int j = lonNumberStart;j <= lonNumberEnd;j++){
//				System.out.println(i + ":" + j);
				list.add(ZOrderValue.parseToZOrder(i, j));
			}
		}
		
		return list;
	}
	
	
	/*
	 * the cell start from 0 to n-1
	 * for the S set, the tile is larger than the tile of R set one threshold width
	 */
	public static ArrayList<Integer> tileNumberOfS(double lat, double lon){
		ArrayList<Integer> list = new ArrayList<Integer>();
		double tileWidth = FlickrSimilarityUtil.wholeSpaceWidth / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		double tileHight = FlickrSimilarityUtil.WholeSpaceLength / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/tileWidth);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/tileHight);
//		list.add(ZOrderValue.parseToZOrder(latNumber, lonNumber));
		
		ArrayList<Integer> latList = new ArrayList<Integer>();
		ArrayList<Integer> lonList = new ArrayList<Integer>();
		
		latList.add(latNumber);
		lonList.add(lonNumber);
		
		/*
		 * get S lat tile list
		 */
		if( Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + latNumber*tileWidth,lon)< FlickrSimilarityUtil.DISTANCE_THRESHOLD ){
			
			if(latNumber != 0){
				latList.add(latNumber-1);
			}
		}
		
		if(Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + (latNumber+1)*tileWidth,lon) < FlickrSimilarityUtil.DISTANCE_THRESHOLD){
			latList.add(latNumber+1);
		}
		
		
		/*
		 * get S lon tile list
		 */
		if( Distance.GreatCircleDistance(lat,lon,lat,FlickrSimilarityUtil.MIN_LON + lonNumber*tileHight)< FlickrSimilarityUtil.DISTANCE_THRESHOLD ){
			if(lonNumber != 0){
				lonList.add(lonNumber-1);
			}
		}
		
		if(Distance.GreatCircleDistance(lat,lon,lat,FlickrSimilarityUtil.MIN_LON + (lonNumber+1)*tileHight) < FlickrSimilarityUtil.DISTANCE_THRESHOLD){
			lonList.add(lonNumber+1);
		}
		
		for(Integer la: latList){
			for(Integer lo: lonList){
				list.add(ZOrderValue.parseToZOrder(la, lo));
			}
		}
		
		return list;
	}
	
	/*
	 * extend the point at threshold width
	 */
	public static ArrayList<Integer> tileNumberList(double lat,double lon){
		ArrayList<Integer> list = new ArrayList<Integer>();
		
		
		return list;
		
	}
	
	/**
	 * 
	 * @param tileNumber
	 * @return the partition number using the round robin algorithm
	 */
	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.PARTITION_NUMBER;
		
	}
	
	private IntWritable outputKey = new IntWritable();
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
		
		String textual = value.toString().split(":")[5];

		ArrayList<Integer> tileList = new ArrayList<Integer>();
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		outputValue.setTimestamp(timestamp);
//		outputValue.setTextual(textual);
		
		
		
		if(tag == FlickrSimilarityUtil.R_tag){
			
			tileList = tileNumberOfR(lat,lon);
		}else{
//			tileList = tileNumberOfS(lat,lon);
			tileList = tileOfS(lat,lon);
		}
		
//		System.out.println(tileList.toString());
		
//		outputValue.setTiles(tileList.toString().substring(1, tileList.toString().length()-1));
		
		outputValue.setTiles(value.toString().split(":")[5]);
		
//		System.out.println(outputValue.getTiles());
		/*
		 * for R, there is only need one tile
		 * but for S, the data should send to other tiles
		 */
		for(Integer tile: tileList){
		
			outputValue.setTileNumber(tile);
			outputKey.set(GridPartition.paritionNumber(tile));
//			outputKey.set(tile);
			context.write(outputKey, outputValue);
			
		}
		
	}
	
	
	
}
