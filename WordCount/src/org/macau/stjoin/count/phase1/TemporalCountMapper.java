package org.macau.stjoin.count.phase1;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 */
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TemporalCountMapper extends
	Mapper<Object, Text, Text, IntWritable>{
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal Count Mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		if(tag == FlickrSimilarityUtil.R_tag){
			context.write(new Text(timeInterval + ""), new IntWritable(1));
		}

		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal Count Mapper end at " + System.currentTimeMillis() + "\n");
	}
}