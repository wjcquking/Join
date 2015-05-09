package org.macau.flickr.spatial.minimal;

/**
 * @author: wangjian
 * the idea comes from the paper "Minimal MapReduce Algorithms",SIGMOD, 2013
 * 
 * The mapper read the local data and then sort in another dimension
 */


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.flickr.spatial.partition.*;


public class MiniSpatialJoinMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	
	private IntWritable outputKey = new IntWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		
		if(id == 65480044){
			
		}
		
		
		int  tileNumber = 1;
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tileNumber);
		outputValue.setTimestamp(timestamp);
		
		outputKey.set(GridPartition.paritionNumber(tileNumber));
		context.write(outputKey, outputValue);
		
	}
}
