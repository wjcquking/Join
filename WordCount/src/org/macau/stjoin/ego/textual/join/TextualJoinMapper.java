package org.macau.stjoin.ego.textual.join;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
		
//		int timeInterval = outputValue.getTiles().split(";").length;
			
		outputValue.setTag(tag);
		
		
		long[][] bounds = {{2L,2114,112},{9L,2114,112},{18L,2114,112},{28L,2114,112},{42L,2114,112},{61L,2114,112},{88L,2114,112},{123L,2114,112},{168L,2114,112},{224L,2114,112},{287L,2114,112},{361L,2114,112},{451L,2114,112},{561L,2114,112},{698L,2114,112},{868L,2114,112},{1073L,2114,112},{1322L,2114,112},{1628L,2114,112},{2016L,2114,112},{2513L,2114,112},{3139L,2114,112},{3907L,2114,112},{4859L,2114,112},{6060L,2114,112},{7633L,2114,112},{9724L,2114,112},{12554L,2114,112},{16602L,2114,112},{22815L,2114,112}};
		
//		long[][] bounds = {{2L,2114,112},{5L,2114,112},{16L,2114,112},{26L,2114,112},{42L,2114,112},{61L,2114,112},{88L,2114,112},{123L,2114,112},{168L,2114,112},{224L,2114,112},{287L,2114,112},{361L,2114,112},{451L,2114,112},{561L,2114,112},{698L,2114,112},{868L,2114,112},{1073L,2114,112},{1322L,2114,112},{1628L,2114,112},{2016L,2114,112},{2513L,2114,112},{3139L,2114,112},{3907L,2114,112},{4859L,2114,112},{6060L,2114,112},{7633L,2114,112},{9724L,2114,112},{12554L,2114,112},{16602L,2114,112},{22815L,2114,112}};
		
		//selectivity is 0.0001 and computation
//		long[][] bounds = {{2L,2114,112},{3L,2114,112},{5L,2114,112},{7L,2114,112},{10L,2114,112},{21L,2114,112},{25L,2114,112},{26L,2114,112},{27L,2114,112},{28L,2114,112},{29L,2114,112},{31L,2114,112},{38L,2114,112},{42L,2114,112},{51L,2114,112},{60L,2114,112},{69L,2114,112},{75L,2114,112},{81L,2114,112},{120L,2114,112},{190L,2114,112},{271L,2114,112},{320L,2114,112},{451L,2114,112},{629L,2114,112},{901L,2114,112},{1310L,2114,112},{1883L,2114,112},{3106L,2114,112},{6451L,2114,112}};

		
		//computation
//		long[][] bounds = {{2L,2114,112},{3L,2114,112},{4L,2114,112},{7L,2114,112},{9L,2114,112},{21L,2114,112},{25L,2114,112},{26L,2114,112},{27L,2114,112},{28L,2114,112},{31L,2114,112},{38L,2114,112},{42L,2114,112},{51L,2114,112},{60L,2114,112},{69L,2114,112},{75L,2114,112},{78L,2114,112},{112L,2114,112},{164L,2114,112},{226L,2114,112},{292L,2114,112},{365L,2114,112},{500L,2114,112},{704L,2114,112},{988L,2114,112},{1420L,2114,112},{2041L,2114,112},{3413L,2114,112},{6788L,2114,112}};
		
		
		if(!outputValue.getTiles().equals("null")){
			int pNumber = 0;
			
			String[] textualList = outputValue.getTiles().split(";");
			
			
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			Set<Integer> partitionSet = new HashSet<Integer>();
			
			System.out.println("Textual " +outputValue.getTiles());
			System.out.println(prefixLength);
			
			for(int i = textualList.length-1; i >= textualList.length-prefixLength;i--){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				
				if(tokenID >= bounds[bounds.length-1][0]){
					
					pNumber = bounds.length;
					
				}else{
					
					for(int j = 0; j < bounds.length;j++){
						
						if(tokenID < bounds[j][0]){
							pNumber = j;
							break;
						}
					}
				}
				
				if(pNumber == 0){
					pNumber  = 1;
				}
				System.out.println(i + ":"+tokenID + ":" + pNumber + ":" + partitionSet);
				partitionSet.add(pNumber);
				
			}
			
			System.out.println((textualList.length-prefixLength) + ":" + partitionSet);
			
			//for the R set
			
			for(Integer temp : partitionSet){
				
				outputKey.set(temp);
				
				outputValue.setTileNumber(prefixLength);
				
				context.write(outputKey, outputValue);
			}
			partitionSet.clear();
		}
	
		
	}

}
