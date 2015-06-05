package org.macau.stjoin.count.selectivity;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.util.DataSimilarityUtil;

public class SelectivityCountMapper extends
	Mapper<Object, Text, LongWritable, IntWritable>{
	
	private final LongWritable outputKey = new LongWritable();
	private final FlickrValue outputValue = new FlickrValue();

	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal Count Mapper Start at " + System.currentTimeMillis());
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
		
		
//		if(tag == FlickrSimilarityUtil.R_tag){
//			context.write(new LongWritable(timeInterval), new IntWritable(1));
//		}else{
//			
//		}
		
		
		if(!outputValue.getTiles().equals("null")){
			if(tag == FlickrSimilarityUtil.S_tag){
				
				for(int i = -1; i<=1;i++){
					outputKey.set(timeInterval+ i);
					context.write(outputKey, new IntWritable(tag));
				}
				
			}else{
				
				//for the R set
				outputKey.set(timeInterval);
				context.write(outputKey, new IntWritable(tag));
			}
		}

		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal Count Mapper end at " + System.currentTimeMillis() + "\n");
	}
}