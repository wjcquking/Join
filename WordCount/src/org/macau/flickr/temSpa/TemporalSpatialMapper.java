package org.macau.flickr.temSpa;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author hadoop
 * Read the raw data
 * think how to partition the data to finish all the job in one mapreduce job
 */
public class TemporalSpatialMapper extends 
Mapper<Object, Text, Text,IntWritable>{
	
//	private final long TIME_PERIOD = 1000;
	
	private final Text outputKey = new Text();
	
	private final static IntWritable one = new IntWritable(1);
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		String wholeDate = df.format(date);
		return wholeDate.split("-")[0];
	}
	
	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
		
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		
		outputKey.set(convertDateToString(new Date(timestamp)));
		
		context.write(outputKey, one);
		
	}

}