package org.macau.stjoin.basic.temporal.improved;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 * Modified Date : 2015-05-21
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.FlickrValueWithMultiFeature;
import org.macau.stjoin.util.DataSimilarityUtil;

public class TemporalJoinMapper extends
	Mapper<Object, Text, LongWritable, FlickrValueWithMultiFeature>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final FlickrValueWithMultiFeature outputValue = new FlickrValueWithMultiFeature();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal mapper Start at " + System.currentTimeMillis());
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
		
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		
		DataSimilarityUtil.getFlickrValue(outputValue, value.toString());
		
		
		long timeInterval = outputValue.getTimestamp() / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setTag(tag);
		
		

		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, it should set to three time interval
		if(!outputValue.getTiles().equals("null")){
			if(tag == FlickrSimilarityUtil.S_tag){
				
				for(int i = -1; i<=1;i++){
					outputKey.set(timeInterval+ i);
					outputValue.setTileNumber((int)timeInterval + i);
					context.write(outputKey, outputValue);
				}
				
			}else{
				
				//for the R set
				outputKey.set(timeInterval);
				context.write(outputKey, outputValue);
			}
		}
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n");
	}
}