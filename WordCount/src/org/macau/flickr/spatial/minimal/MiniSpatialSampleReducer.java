package org.macau.flickr.spatial.minimal;

/**
 * There are two step
 * Filter Step: Input: the tuples belongs to the same partitions
 * the goal is to find the paris of intersecting rectangles between the two sets by strip-based plane sweeping techniques.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.ComparatorFlickrValue;

public class MiniSpatialSampleReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		
		int count = 0;
		
		for(FlickrValue value:values){
			count++;
			/*We need new a FlickrValue, if not, the values in Map.getTag() will become the same
			 * this may because the address of the value is the same, when change a value, all the values
			 * in the Map.getTag will become the same
			 * 
			 * */
			FlickrValue fv = new FlickrValue(value);
			records.add(fv);
		
		}
		
		System.out.println("count" + count);
		ComparatorFlickrValue comparator = new ComparatorFlickrValue();
		Collections.sort(records, comparator);
		
		context.write(new Text(records.get(count/FlickrSimilarityUtil.MACHINE_NUNBER).getLat()+" "), new Text(" "));
		context.write(new Text(records.get(count/FlickrSimilarityUtil.MACHINE_NUNBER*2).getLat()+" "), new Text(" "));
		
		for(int i = 0; i< records.size();i++){
			System.out.println(records.get(i).getLat() + ": " + records.get(i).getId() + " " +records.get(i).getTag());
		}
		

	}
	
}
