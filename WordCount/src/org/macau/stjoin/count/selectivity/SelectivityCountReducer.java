package org.macau.stjoin.count.selectivity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;


public class SelectivityCountReducer extends
	Reducer<LongWritable, IntWritable, LongWritable, LongWritable>{
		
		private final LongWritable outputValue = new LongWritable(0);
	
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal Count reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(LongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException{
			
			long sCount = 0;
			long rCount = 0;
			int sum = 0;
			for(IntWritable value:values){
				if(value.get() == FlickrSimilarityUtil.S_tag){
					sCount++;
				}else{
					rCount++;
				}
			}
//			outputValue.set(outputValue.get() + rCount*sCount);
			outputValue.set(rCount*sCount);
			
			if(rCount*sCount > 0){
				context.write(key, outputValue);
			}

		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("clean up");
			System.out.println("The Temporal Count Reducer End at "+System.currentTimeMillis());

		}
	}

