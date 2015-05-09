package org.macau.flickr.temporal.analysis;

/**
 * Simple MapReduce Job which can account the time period
 * and the number of each time point
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;

import org.apache.hadoop.io.Text;


public class TemporalAccount {

	public static boolean TemporalAccountJob(Configuration conf) throws Exception{
		
		Job accountJob = new Job(conf, "Temporal Account Job");
		accountJob.setJarByClass(TemporalAccount.class);
		
		accountJob.setMapperClass(TemporalAccountMapper.class);
		//accountJob.setCombinerClass(TemporalAccountReducer.class);
		accountJob.setReducerClass(TemporalAccountReducer.class);
		
		accountJob.setOutputKeyClass(LongWritable.class);
		accountJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(accountJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(accountJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(accountJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
