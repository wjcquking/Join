package org.macau.stjoin.mapper.basic.phase1;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;

public class TemporalJoinMapper extends
	Mapper<Object, Text, LongWritable, Text>{
	
	private final LongWritable outputKey = new LongWritable();
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Mapper side join mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		

		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, it should set to three time interval
		
		if(tag == FlickrSimilarityUtil.S_tag){
			
			for(int i = -1; i<=1;i++){
				outputKey.set(timeInterval+ i);
				context.write(outputKey, new Text(tag + ":" + value));
			}
			
		}else{
			//for the R set
			outputKey.set(timeInterval);
			context.write(outputKey, new Text(tag + ":" + value));
		}
		
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The map side join mapper end at " + System.currentTimeMillis() + "\n");
	}
}