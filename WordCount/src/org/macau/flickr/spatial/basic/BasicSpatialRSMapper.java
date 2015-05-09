package org.macau.flickr.spatial.basic;

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
import org.macau.flickr.spatial.partition.GridPartition;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.spatial.Distance;

/**
 * 
 * @author mb25428
 * Read the flickr data, then extract the spatial information, send the object to other machine
 * For Example
 * The Data form:ID;lat;lon;timestamp
 * The Data Example:1093113743;48.89899;2.380696;973929974000
 */


public class BasicSpatialRSMapper extends
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
		tileList = GridPartition.tileNumberByMultiAssignment(lat, lon);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		outputValue.setTimestamp(timestamp);
		outputValue.setTiles("");
		
		for(Integer tile: tileList){
			
			outputValue.setTileNumber(tile);
			outputKey.set(tile);
			context.write(outputKey, outputValue);
			
		}
		
	}
	
	
	
}
