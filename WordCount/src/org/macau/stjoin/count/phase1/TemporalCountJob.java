package org.macau.stjoin.count.phase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;


/**
 * 
 * @author hadoop
 * Word Count
 * Calculate the count number for each timeinterval
 * Modify Date: 2015-01-17
 */
public class TemporalCountJob {

	public static boolean TemporalCount(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Count Job");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(TemporalCountMapper.class);
		basicJob.setCombinerClass(TemporalCountReducer.class);
		
		basicJob.setReducerClass(TemporalCountReducer.class);
		
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
