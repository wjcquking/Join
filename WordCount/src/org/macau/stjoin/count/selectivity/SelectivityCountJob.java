package org.macau.stjoin.count.selectivity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;


/**
 * 
 * @author hadoop
 * Selectivity Count
 * 
 * This file is for the selectivity of the data
 * 
 * Mapper output
 * Key: the candidate tags
 * Value: the file tag: R or S
 * 
 * Reducer Output
 * Key: the canidate tags
 * Value: the sum of the Project of the R and S in this tags
 * 
 * Modify Date: 2015-01-17
 * 
 */
public class SelectivityCountJob {

	public static boolean SelectivityCount(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Selectivity Count Job");
		basicJob.setJarByClass(SelectivityCountJob.class);
		
		basicJob.setMapperClass(SelectivityCountMapper.class);
//		basicJob.setCombinerClass(SelectivityCountReducer.class);
		
		basicJob.setReducerClass(SelectivityCountReducer.class);
		
		basicJob.setMapOutputKeyClass(LongWritable.class);
		basicJob.setMapOutputValueClass(IntWritable.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
