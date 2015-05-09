package org.macau.flickr.temporal.analysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.macau.flickr.temporal.TemporalUtil;

public class TemporalAccountMapper extends 
Mapper<Object, Text, LongWritable,IntWritable>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final static IntWritable one = new IntWritable(1);
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		String wholeDate = df.format(date);
		return wholeDate.split("-")[0];
	}
	
	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
		
		long timestamp = Long.parseLong(value.toString().split(";")[3])/TemporalUtil.MS_OF_ONE_DAY;
		
		outputKey.set(timestamp);
		
		context.write(outputKey, one);
		
	}

}
