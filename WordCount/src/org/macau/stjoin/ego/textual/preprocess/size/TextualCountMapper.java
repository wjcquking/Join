package org.macau.stjoin.ego.textual.preprocess.size;

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
		String textual = value.toString().split(":")[5];
		String[] textualList = textual.split(";");
		
		
		
		if(!textual.equals("null")){
			
			
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			int token = Integer.parseInt(textualList[textualList.length-prefixLength]);
			
			String outputValue = String.format("%04d", textualList.length) + ":" + String.format("%04d", token);
			
			context.write(new Text(outputValue), new LongWritable(1));
			
		}
		
		
		
		
		
		//for the computation
//		if(!textual.equals("null")){
//		
//			String size = "";
//			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
//			
//			/*
//			 * Because in the textual information, the token is ordered from the high to the low
//			 * So when calculate the prefix, we take the token from the end to the first.
//			 */
//			
//			String lastValue = textualList[textualList.length-prefixLength];
//			
//			if(tag == FlickrSimilarityUtil.R_tag){
//				
//				if(textualList.length < 10){
//					size = "0" + textualList.length;
//				}else{
//					size = ""  + textualList.length;
//				}
//				
//				String outputValue = size+ ":" + lastValue;
//				
//				context.write(new Text(outputValue), new LongWritable(tag));
//				
//			}else{
//				
//				int upperBound = (int) (textualList.length / FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
//				int lowerBound = (int) (textualList.length * FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
//				
//				for(int i = lowerBound;i <= upperBound;i++){
//
//					if(i < 10){
//						size = "0" + i;
//					}else{
//						size = "" + i;
//					}
//					String outputValue = size + ":" + lastValue;
//					
//					context.write(new Text(outputValue), new LongWritable(tag));
//				}
//			}
//		}
		
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal Count Mapper end at " + System.currentTimeMillis() + "\n");
	}
}