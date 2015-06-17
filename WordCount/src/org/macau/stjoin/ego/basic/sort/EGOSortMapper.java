package org.macau.stjoin.ego.basic.sort;

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

public class EGOSortMapper extends
	Mapper<Object, Text, Text, Text>{
	
	private final Text outputKey = new Text();
	
	private final Text outputValue = new Text();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("mapper Start at " + System.currentTimeMillis());
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		String[] flickrData = value.toString().split(":");
        
		
		String textual = flickrData[5];
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		

		///threshold is 1 and computation
//		int[][] bounds = {{14278,2114,112},{14616,2114,112},{14655,2114,112},{14683,2114,112},{14696,2114,112},{14712,2114,112},{14728,2114,112},{14746,2114,112},{14763,2114,112},{14777,2114,112},{14790,2114,112},{14801,2114,112},{14812,2114,112},{14821,2114,112},{14828,2114,112},{14835,2114,112},{14841,2114,112},{14847,2114,112},{14853,2114,112},{14858,2114,112},{14863,2114,112},{14868,2114,112},{14872,2114,112},{14877,2114,112},{14882,2114,112},{14886,2114,112},{14890,2114,112},{14893,2114,112},{14897,2114,112},{14899,2114,112}};
		
		//threshold is 0.0005 
		int[][] bounds = {{17437,2114,112},{30296,2114,112},{30706,2114,112},{30974,2114,112},{31069,2114,112},{31201,2114,112},{31330,2114,112},{31486,2114,112},{31635,2114,112},{31736,2114,112},{31821,2114,112},{31911,2114,112},{31965,2114,112},{32015,2114,112},{32084,2114,112},{32175,2114,112},{32253,2114,112},{32378,2114,112},{32462,2114,112},{32572,2114,112},{32657,2114,112},{32724,2114,112},{32780,2114,112},{32834,2114,112},{32931,2114,112},{32991,2114,112},{33130,2114,112},{33292,2114,112},{33447,2114,112},{33575,2114,112}};
		
		//threshold is  and data size
		
		
		if(!textual.equals("null")){
		
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
				
				if(pNumber == 0){
					pNumber  = 1;
				}
				
			}
			
			//for the R set
			outputKey.set(tag + "  " +pNumber);
			
			String record =  flickrData[0] + ":" + flickrData[1] + ":" + flickrData[2] + ":" + flickrData[3] + ":" + flickrData[4]+ ":" + flickrData[5] + ":" + flickrData[6];
			
			outputValue.set(record);
			context.write(outputKey, outputValue);
		}
		
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n" );
	}
}