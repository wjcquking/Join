package org.macau.flickr.temporal.basicImprove;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrValue;

public class TemporalJoinMapper extends
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
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		
		/* Convert the timestamp to the Date
		 * use the day as key
		 * the all value as a value
		 * use the timestamp to refine and compare the distance
		 */
		
//		long previousTimeStamp = timestamp - MS_OF_ONE_DAY;
//		long laterTimestamp = timestamp + MS_OF_ONE_DAY;
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		outputKey.set(timestamp);
		context.write(outputKey, outputValue);
		
		
	}
}