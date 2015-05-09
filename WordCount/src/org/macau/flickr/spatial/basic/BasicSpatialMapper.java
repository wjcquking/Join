package org.macau.flickr.spatial.basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.spatial.partition.GridPartition;
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
 */
public class BasicSpatialMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{
	
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
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		
		outputKey.set(GridPartition.tileNumber(lat, lon));
		
		context.write(outputKey, outputValue);
		
	}
}
