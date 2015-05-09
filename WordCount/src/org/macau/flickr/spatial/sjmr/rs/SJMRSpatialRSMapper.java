package org.macau.flickr.spatial.sjmr.rs;

/**
 * @author: wangjian
 * @date: 2013-11-12
 * 
 * the idea comes from the paper "SJMR:Parallelizing Spatial Join with MapReduce",IEEE, 2009
 * 
 * The MapFunction is for R join S
 * read the data from two set R and S, and join the data
 */


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
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
 * The Data form:ID;lat;lon;timestamp
 * The Data Example:1093113743;48.89899;2.380696;973929974000
 */


public class SJMRSpatialRSMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	/*
	 * use the z curve order and round-robin algorithm to find the best partition function
	 * the universe is divided regularly into Nt tiles
	 * each tile is number from 0 to Nt-1 according to z curve, 
	 * and mapped to a partition p with a round robin scheme
	 * 
	 */

	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("setup");
	}
	
	public static int tileNumber(double lat,double lon){
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		return ZOrderValue.parseToZOrder(latNumber, lonNumber);
		
	}
	
	/*
	 * the cell start from 0 to n-1
	 * for the S set, the tile is larger than the tile of R set one threshold width
	 */
	public static ArrayList<Integer> tileNumberOfS(double lat, double lon){
		ArrayList<Integer> list = new ArrayList<Integer>();
		double tileWidth = FlickrSimilarityUtil.wholeSpaceWidth / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		double tileHight = FlickrSimilarityUtil.WholeSpaceLength / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
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
	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.PARTITION_NUMBER;
		
	}
	
	private IntWritable outputKey = new IntWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
		
		//R: 0; S:1
		int tag;
		
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		
		if(fileName.contains(FlickrSimilarityUtil.R_TAG)){
			
			tag = 0;
			
		}else{
			tag = 1;
		}
		
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		ArrayList<Integer> tileList = new ArrayList<Integer>();
		tileList = tileNumberOfS(lat,lon);

		
		int  tileNumber = tileNumber(lat,lon);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		outputValue.setTimestamp(timestamp);
		outputValue.setTiles("");
		//outputValue.setTileNumber(tileNumber);
		
//		outputKey.set(GridPartition.paritionNumber(tileNumber));
//		context.write(outputKey, outputValue);
		for(Integer tile: tileList){
			
			outputValue.setTileNumber(tile);
//			outputKey.set(GridPartition.paritionNumber(tile));
			outputKey.set(tile);
			context.write(outputKey, outputValue);
			
		}
		
	}
	
	
	
}
