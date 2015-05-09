package org.macau.flickr.temporal.analysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author hadoop
 * input:
 * KEY:  Date String
 * Value: accounts
 * 
 * output:
 * KEY: Date String
 * Value : accounts sum
 */
public class TemporalAccountReducer extends
	Reducer<LongWritable,IntWritable,  IntWritable, LongWritable>{
	
	private IntWritable result = new IntWritable();
	
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		
		
		int sum = 0;
		for(IntWritable val : values){
			sum += val.get();
		}
		result.set(sum);
		context.write(result, key);
		
	}

}
