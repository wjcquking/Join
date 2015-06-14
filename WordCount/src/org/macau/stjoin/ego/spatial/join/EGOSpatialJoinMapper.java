package org.macau.stjoin.ego.spatial.join;

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
import org.macau.flickr.util.FlickrValue;
import org.macau.stjoin.util.DataSimilarityUtil;

public class EGOSpatialJoinMapper extends
	Mapper<Object, Text, LongWritable, FlickrValue>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("mapper Start at " + System.currentTimeMillis());
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		DataSimilarityUtil.getFlickrValue(outputValue, value.toString());
		
		long timeInterval = outputValue.getTimestamp() / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		long tile = 1;
		
		
		outputValue.setTileNumber((int)timeInterval);
		outputValue.setTag(tag);
		
		
		
		//the selectivity is 0.01 and data size
		int[][] bounds ={{342931887,2114,112},{342933349,2114,112},{343020857,2114,112},{343021115,2114,112},{343021117,2114,112},{343021184,2114,112},{343021270,2114,112},{343021948,2114,112},{343022454,2114,112},{343022633,2114,112},{343022699,2114,112},{343022743,2114,112},{343022779,2114,112},{343022782,2114,112},{343022783,2114,112},{343022788,2114,112},{343022818,2114,112},{343022836,2114,112},{343023115,2114,112},{343023124,2114,112},{343023128,2114,112},{343023146,2114,112},{343023152,2114,112},{343023168,2114,112},{343023225,2114,112},{343023281,2114,112},{343023384,2114,112},{343028021,2114,112},{343028767,2114,112},{343029000,2114,112}};
		

		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, copy to other partition in the bound
		
		
		
		/****************************************************************
		 * 
		 * 
		 * The Partition Method
		 * 
		 * 
		 * 
		 ****************************************************************/
		
		if(!outputValue.getTiles().equals("null")){
			
			if(tag == FlickrSimilarityUtil.S_tag){
				
				int pNumber = 0;
				
				if(timeInterval >= bounds[bounds.length-1][0]){
					
					pNumber = bounds.length;
					
				}else{
					
					for(int i = 0; i < bounds.length;i++){
						
						if(timeInterval < bounds[i][0]){
							pNumber = i;
							break;
						}
					}
				}
				
				
				if(pNumber == 0){
					pNumber = 1;
				}
				outputKey.set(pNumber);
				outputValue.setTileNumber((int)timeInterval );
				context.write(outputKey, outputValue);
				
				
				
				if(pNumber == bounds.length){
					if(timeInterval- bounds[bounds.length-1][0] == 0){
						outputKey.set(pNumber-1);
						outputValue.setTileNumber((int)timeInterval );
						context.write(outputKey, outputValue);
					}
				}
				
				
				if(pNumber >= 1 && pNumber <= bounds.length-1){
					
					if(timeInterval- bounds[pNumber-1][0] == 0){
						outputKey.set(pNumber-1);
						outputValue.setTileNumber((int)timeInterval );
						context.write(outputKey, outputValue);
					}
					
					
					if(timeInterval- bounds[pNumber][0] == -1){
						outputKey.set(pNumber+1);
						outputValue.setTileNumber((int)timeInterval );
						context.write(outputKey, outputValue);
					}
				}
				
				
			}else{
				
				int pNumber = 0;
				
				if(timeInterval >= bounds[bounds.length-1][0]){
					
					pNumber = bounds.length;
					
				}else{
					
					for(int i = 0; i < bounds.length;i++){
						
						if(timeInterval < bounds[i][0]){
							pNumber = i;
							break;
						}
					}
				}
				
				if(pNumber == 0){
					pNumber  = 1;
				}
				//for the R set
				outputKey.set(pNumber);
				outputValue.setTileNumber((int)timeInterval);
				context.write(outputKey, outputValue);
			}
			}
		
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The mapper end at " + System.currentTimeMillis() + "\n" );
	}
}