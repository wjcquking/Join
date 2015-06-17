package org.macau.stjoin.ego.basic.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class EGOSortReducer extends
	Reducer<Text, Text,Text , Text>{
		
		

		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("The reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException{
			
			
			
			for(Text value:values){
				
				context.write(key, new Text(value.toString()));
				
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

