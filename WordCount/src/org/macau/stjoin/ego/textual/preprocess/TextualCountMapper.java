package org.macau.stjoin.ego.textual.preprocess;

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
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.util.SimilarityUtil;

public class TextualCountMapper extends
	Mapper<Object, Text, Text, LongWritable>{
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal Count Mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		//simple version only data size
		
		
		
		//for the data size
		//use only the data size
		String textual = value.toString().split(":")[5];
		
		if(!textual.equals("null")){
			
			String[] textualList = textual.split(";");
			
//			String outputValue = textualList.length + ":" + textualList[0];
			String outputValue = textualList.length + "";
			
			
			context.write(new Text(outputValue), new LongWritable(tag));
			
		}
		
		String[] textualList = textual.split(";");
		
		
		
		//for the computation
		// use the data size
		if(!textual.equals("null")){
		
			if(tag == FlickrSimilarityUtil.R_tag){
				
				
				
				String outputValue = textualList.length + "";
				
				context.write(new Text(outputValue), new LongWritable(tag));
				
			}else{
				
				int upperBound = (int) (textualList.length / FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
				int lowerBound = (int) (textualList.length * FlickrSimilarityUtil.TEMPORAL_THRESHOLD);
				for(int i = lowerBound;i <= upperBound;i++){

					String outputValue = textualList.length + "";
					
					context.write(new Text(outputValue), new LongWritable(tag));
				}
			}
		}
		
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal Count Mapper end at " + System.currentTimeMillis() + "\n");
	}
}