package org.macau.stjoin.count.phase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

import org.macau.stjoin.basic.temporal.TemporalComparator;;


public class TemporalCountReducer extends
	Reducer<Text, IntWritable, Text, IntWritable>{
		

		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			for(IntWritable value:values){
				sum += value.get();
			}
			
			context.write(key, new IntWritable(sum));

		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("clean up");
			System.out.println("The Reducer End at"+System.currentTimeMillis());

		}
	}

