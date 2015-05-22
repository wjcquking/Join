package org.macau.stjoin.mapper.improved.phase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;


/**
 * 
 * @author hadoop
 * This is for the mapper join
 * Phase 2
 * the mapper read the data, and the same key in the same reducer
 * 
 */
public class MapperSideJoinPhase2Job {

	public static boolean MapperSideBasicJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Basic Similarity Join");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(MapperSideJoinMapper.class);
		basicJob.setReducerClass(MapperSideJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(LongWritable.class);
		basicJob.setMapOutputValueClass(FlickrValue.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrResultPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
