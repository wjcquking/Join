package org.macau.flickr.spatial.analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpatialAccountReducer extends
	Reducer<DoubleWritable,IntWritable, DoubleWritable,IntWritable>{

	private IntWritable result = new IntWritable();
	
	public void reduce(DoubleWritable key,Iterable<IntWritable> values, Context context)
		throws IOException,InterruptedException{
		int sum = 0;
		
		for(IntWritable val : values){
			sum += val.get();
		}
		
		result.set(sum);
		context.write(key, result);
	}
	
}
