package org.macau.flickr.temSpa;


/**
 * The Reducer will get the temporal result
 * and Refine the results of temporal with the spatial feature
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class TemporalSpatialReducer extends
Reducer<Text,IntWritable, Text, IntWritable>{

	private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		
		int sum = 0;
		for(IntWritable val : values){
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
		
	}

}
