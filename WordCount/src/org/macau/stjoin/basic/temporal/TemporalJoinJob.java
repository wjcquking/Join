package org.macau.stjoin.basic.temporal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;


/**
 * 
 * @author hadoop
 * Map: Input:  KEY  : Record
 *              Value: Record String
 *      output: KEY  : The Time / threshold
 *      		Value: The FlickrValue
 *      
 * Reduce: Input: KEY  : 
 * 				  Value:
 * 
 * 
 * Modify date: 2015-05-24
 * 
 */
public class TemporalJoinJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Basic Similarity Join");
		basicJob.setJarByClass(TemporalJoinJob.class);
		
		basicJob.setMapperClass(TemporalJoinMapper.class);
		
		basicJob.setReducerClass(TemporalJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(LongWritable.class);
		basicJob.setMapOutputValueClass(FlickrValue.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
