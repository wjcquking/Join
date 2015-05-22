package org.macau.stjoin.mapper.improved.phase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;


/**
 * 
 * @author hadoop
 * This is for the mapper join
 * Phase 1
 * the mapper read the data, and the same key to the same reducer
 */
public class MapSideJoinJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Basic Similarity Join");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(TemporalJoinMapper.class);
		
		basicJob.setReducerClass(TemporalJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(LongWritable.class);
		basicJob.setMapOutputValueClass(Text.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
