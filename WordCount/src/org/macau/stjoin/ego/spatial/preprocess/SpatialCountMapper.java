package org.macau.stjoin.ego.spatial.preprocess;

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
import org.macau.stjoin.util.DataSimilarityUtil;

public class SpatialCountMapper extends
	Mapper<Object, Text, Text, IntWritable>{
	
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal Count Mapper Start at " + System.currentTimeMillis());
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		
		
		
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int x = (int) (lat/thres);
		int y = (int)(lon/thres );
		
		
		int zorder = ZOrderValue.parseToZOrder(x, y);
		
		String outputValue = zorder + ":" + x + ":" + y;
		
		context.write(new Text(outputValue), new IntWritable(1));
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal Count Mapper end at " + System.currentTimeMillis() + "\n");
	}
}