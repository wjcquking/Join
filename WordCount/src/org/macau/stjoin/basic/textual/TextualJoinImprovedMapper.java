package org.macau.stjoin.basic.textual;

/***************************************************************
 *
 *
 * output Key: get some keywords together as one key, the key number is like the partition number; 
 * 
 *************************************************************/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.util.SimilarityUtil;



public class TextualJoinImprovedMapper extends
	Mapper<Object, Text, IntWritable, FlickrValue>{
		
		
	private final IntWritable outputKey = new IntWritable();
	private FlickrValue outputValue = new FlickrValue();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		outputValue = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));
		
		
		outputValue.setTag(tag);
		
		
		String textual = value.toString().split(":")[5];
		
		
		if(!textual.equals("null")){
			
			String[] textualList = textual.split(";");
			
			
			//get the prefix values
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			for(int i = 0; i < prefixLength;i++){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				
				outputValue.setTileNumber(tokenID);
				
				outputKey.set(tokenID % FlickrSimilarityUtil.PARTITION_NUMBER);
				context.write(outputKey, outputValue);
				
			}
			
		}
		
	
		
	}

}
