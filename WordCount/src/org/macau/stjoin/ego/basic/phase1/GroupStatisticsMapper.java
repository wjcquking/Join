package org.macau.stjoin.ego.basic.phase1;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 * Modify Date: 2015-05-17
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;

public class GroupStatisticsMapper extends
	Mapper<Object, Text, LongWritable, Text>{
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		

		value = new Text(tag + " " + timeInterval + " "+ value.toString());
		
		
		if(tag == FlickrSimilarityUtil.S_tag){
			
			
		}else{
			context.write(new LongWritable(timeInterval), value);
		}
	
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n");
	}
}