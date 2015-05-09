package org.macau.flickr.spatial.minimal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author mb25428
 * Read the flickr data, extract the spatial information
 * For Example
 * The Data form:
 * ID;lat;lon;timestamp
 * 1093113743;48.89899;2.380696;973929974000
 * 
 * the boundary objects is 48.857543,48.864375 
 * 
 */

public class MiniSpatialSortMapper extends
Mapper<Object, Text, DoubleWritable, FlickrValue>{

	
	

	
	
	/*
	 * First, each machine use the random sampling algorithm to get the sample
	 * to get the boundary object
	 * Second, order the object according one dimension
	 * So I choose the lat dimension
	 */
	public static int tileNumber(double lat,double lon){
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		return latNumber + lonNumber* FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
	}
	
	/**
	 * 
	 * @param lat
	 * @return the machine number
	 * load the boundary set to decide the machine number which the object should be send to 
	 * 
	 */
	public static int paritionNumber(double lat){
		
		int number = 1;
		return number;
	}
	
	private DoubleWritable outputKey = new DoubleWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	/**
	 * read the data from the local data
	 */
	private void loadBoundarySet(){
		
	}
	
	
	/**
	 * get the boundary data set
	 * and send the data to different 
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

	}
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
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		outputValue.setTag(tag);
		outputValue.setTiles("");
		
		//48.857543,		48.864375 
		// the data is split by the lat and then send to different machine
		if(lat < 48.857543){
			outputKey.set(1);
		}else if(lat > 48.864375 ){
			outputKey.set(3);
		}else{
			outputKey.set(2);
		}
		context.write(outputKey, outputValue);
		
	}
}
