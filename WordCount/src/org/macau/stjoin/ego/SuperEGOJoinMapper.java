package org.macau.stjoin.ego;

/****************************************************
 * 
 * @author hadoop
 * Map: Input:  KEY  : 
 *              Value:
 *      output: KEY  :
 *      		Value:
 *      
 *      
 *      It use the 
 * 		It use some threshold to get the proper value 
 * 		so I need some statistics
 * 
 * Date: 2014-12-29
 ****************************************************/
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SuperEGOJoinMapper extends
	Mapper<Object, Text, Text, FlickrValue>{
	
	private final Text outputKey = new Text();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	private static int sCount = 0;
	private static int rCount = 0;
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal mapper Start at " + System.currentTimeMillis());
	}
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
//		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
//		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		String[] values = value.toString().split("\\s+");
		
		String record = values[3];
		
		int group = Integer.parseInt(values[0]);
		int tag = Integer.parseInt(values[1]);
		
		long id =Long.parseLong(record.toString().split(":")[0]);
		double lat = Double.parseDouble(record.toString().split(":")[2]);
		double lon = Double.parseDouble(record.toString().split(":")[3]);
		long timestamp = Long.parseLong(record.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
//		outputValue = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
		outputValue.setTiles(record.toString().split(":")[5]);
		
//		System.out.println(value.toString().split(":")[5]);
		
		outputValue.setTimestamp(timestamp);
		
		//for the R data
//		int [][] bounds= {{190,2444,119},{195,2444,119,},{199,2444,118},{205,2444,118}};
		
		//For the R and S data
//		int [][] bounds = {{191,2444,118},{196,2444,119},{201,2444,119},{207,2440,117}};
		

		//S set
		if(tag == FlickrSimilarityUtil.S_tag){
			sCount++;
			System.out.println("SS:" + sCount);
//			int group = sCount / 10000;
			
			for(int i = 0; i < 40;i++){
				outputKey.set(group + " " + i);
				outputValue.setTileNumber((int)timeInterval);
				context.write(outputKey, outputValue);
			}
		}else if(tag == FlickrSimilarityUtil.R_tag){
			rCount++;
			System.out.println("RR:" + rCount);
//			int group = rCount / 10000;
			
			for(int i = 0; i < 40;i++){
				outputKey.set(i + " " + group) ;
				outputValue.setTileNumber((int)timeInterval);
				context.write(outputKey, outputValue);
			}
		}
		
//		System.out.println(rCount + "  " + sCount);
		
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int x = (int) (lat /thres);
		int y = (int)(lon/thres );
		

		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, it should set to three time interval
//		if(tag == FlickrSimilarityUtil.S_tag){
//			
//			int pNumber = 0;
//			
//			if(timeInterval >= bounds[bounds.length-1][0]){
//				
//				pNumber = bounds.length;
//				
//			}else{
//				
//				for(int i = 0; i < bounds.length;i++){
//					
//					if(timeInterval < bounds[i][0]){
//						pNumber = i;
//						break;
//					}
//				}
//			}
//			
//			outputKey.set(pNumber);
//			outputValue.setTileNumber((int)timeInterval );
//			context.write(outputKey, outputValue);
//			
//			if(pNumber == 0){
//				if(timeInterval- bounds[0][0] == -1){
//					outputKey.set(pNumber+1);
//					outputValue.setTileNumber((int)timeInterval );
//					context.write(outputKey, outputValue);
//				}
//			}
//			
//			if(pNumber == 4){
//				if(timeInterval- bounds[3][0] == 0){
//					outputKey.set(pNumber-1);
//					outputValue.setTileNumber((int)timeInterval );
//					context.write(outputKey, outputValue);
//				}
//			}
//			
//			
//			if(pNumber >= 1 && pNumber <= 3){
//				
//				if(timeInterval- bounds[pNumber-1][0] == 0){
//					outputKey.set(pNumber-1);
//					outputValue.setTileNumber((int)timeInterval );
//					context.write(outputKey, outputValue);
//				}
//				
//				
//				if(timeInterval- bounds[pNumber][0] == -1){
//					outputKey.set(pNumber+1);
//					outputValue.setTileNumber((int)timeInterval );
//					context.write(outputKey, outputValue);
//				}
//			}
//			
//			
//		}else{
//			
//			int pNumber = 0;
//			
//			if(timeInterval >= bounds[bounds.length-1][0]){
//				
//				pNumber = bounds.length;
//				
//			}else{
//				
//				for(int i = 0; i < bounds.length;i++){
//					
//					if(timeInterval < bounds[i][0]){
//						pNumber = i;
//						break;
//					}
//				}
//			}
//			
//			//for the R set
//			outputKey.set(pNumber);
//			outputValue.setTileNumber((int)timeInterval);
//			context.write(outputKey, outputValue);
//		}
		
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n" );
	}
}