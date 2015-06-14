package org.macau.stjoin.ego.spatial.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SpatialCountReducer extends
	Reducer<Text, IntWritable, Text, IntWritable>{
		

		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal Count reducer Start at " + System.currentTimeMillis());
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
			System.out.println("The Temporal Count Reducer End at "+System.currentTimeMillis());

		}
	}

