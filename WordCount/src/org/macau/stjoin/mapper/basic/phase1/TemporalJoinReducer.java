package org.macau.stjoin.mapper.basic.phase1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TemporalJoinReducer extends
	Reducer<LongWritable, Text, LongWritable, Text>{
	
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal reducer Start at " + System.currentTimeMillis());
			
		}
		
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException{
			
			for(Text value:values){
		
				context.write(key,value);
			}
			
				
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

