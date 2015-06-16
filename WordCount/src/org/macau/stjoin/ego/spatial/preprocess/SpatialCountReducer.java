package org.macau.stjoin.ego.spatial.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;


public class SpatialCountReducer extends
	Reducer<Text, LongWritable, Text, LongWritable>{
		

		private final LongWritable outputValue = new LongWritable(0);
	
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal Count reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException{
			
			
//			long sum = 0;
//			for(LongWritable value:values){
//				sum += value.get();
//			}
//			
//			if(sum > 10){
//				context.write(key, new LongWritable(sum));
//			}
			
			long sCount = 0;
			long rCount = 0;
			
			
			for(LongWritable value:values){
				if(value.get() == FlickrSimilarityUtil.R_tag){
					rCount++;
				}else{
					sCount++;
				}
			}
			System.out.println(key + ":"+rCount + ":" + sCount);
			
			long temp = rCount*sCount;
			
			if(temp > 10){
				context.write(key, new LongWritable(temp));
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

