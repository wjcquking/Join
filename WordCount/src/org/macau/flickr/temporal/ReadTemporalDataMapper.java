package org.macau.flickr.temporal;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author mb25428
 * @date 
 * Last Modify Date: 14/10/2013
 * If we want to find the pictures whose uploading time difference between one day
 */
public class ReadTemporalDataMapper extends
	Mapper<Object, Text, LongWritable, FlickrValue>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	/**
	 * Convert the timestamp to the Date
	 * use the day as key
	 * the all value as a value
	 * use the timestamp to refine and compare the distance
	 */
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//get the basic information
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		

				
		int[] timeArray = {0,1};
		
		outputValue.setId(id);
		outputValue.setLat(lat); 
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		long time = timestamp / TemporalUtil.MS_OF_ONE_DAY;
		for(int i : timeArray){
			outputKey.set(time + i);
			outputValue.setTag(i);
			context.write(outputKey, outputValue);
		}
		
		
	}
}
