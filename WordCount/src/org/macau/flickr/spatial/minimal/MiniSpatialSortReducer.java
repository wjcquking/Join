package org.macau.flickr.spatial.minimal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.ComparatorFlickrValue;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class MiniSpatialSortReducer extends
	Reducer<DoubleWritable, FlickrValue, FlickrValue, Text>{

	private final Text text = new Text();
	private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	public void reduce(DoubleWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		for(FlickrValue value:values){
			FlickrValue recCopy = new FlickrValue(value);
		    records.add(recCopy);
		}
		
		ComparatorFlickrValue comparator = new ComparatorFlickrValue();
		Collections.sort(records, comparator);
		
		for(int i =0;i < records.size();i++){
			System.out.println(records.get(i));
			context.write(records.get(i), new Text(""));
		}
		
		
	}
	
}
