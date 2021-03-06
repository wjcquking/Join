package org.macau.stjoin.basic.textual;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.util.DataSimilarityUtil;
import org.macau.util.SimilarityUtil;


public class TextualJoinMapper extends
	Mapper<Object, Text, IntWritable, FlickrValue>{
	
		
	private final IntWritable outputKey = new IntWritable();
	private FlickrValue outputValue = new FlickrValue();
	

	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		DataSimilarityUtil.getFlickrValue(outputValue, value.toString());
		
			
		outputValue.setTag(tag);
		
		if(!outputValue.getTiles().equals("null")){
			
			String[] textualList = outputValue.getTiles().split(";");
			
			
			//get the prefix values
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			/*
			 * Because in the textual information, the token is ordered from the high to the low
			 * So when calculate the prefix, we take the token from the end to the first.
			 */
			for(int i = textualList.length-1; i >= textualList.length-prefixLength;i--){
//			for(int i =0; i <prefixLength;i++){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				
				outputValue.setTileNumber(tokenID);
				
				outputKey.set(tokenID);
				context.write(outputKey, outputValue);
				
			}
			
		}
		

		
	}

}
