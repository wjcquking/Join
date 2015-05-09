package org.macau.stjoin.basic.temporal.combination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;


/*************************************************
 * 
 * @author hadoop
 * Map: Input:  KEY  : 
 *              Value:
 *      output: KEY  : The feature 
 *      		Value: The Record
 *      
 * Reduce: Input: KEY  :
 * 				  Value:
 * 
 * Desc: Divide the data into two part and for each part,
 * 		 The data use different feature.
 * 
 * 
 * Date: 2014-11-19
 * 
 **************************************************/


public class TemporalSpatialCombinationJoinJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Spatial Combination Similarity Join");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(TemporalSpatialCombinationJoinMapper.class);
		
		basicJob.setReducerClass(TemporalSpatialCombinationJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
		basicJob.setMapOutputValueClass(FlickrValue.class);
		
		basicJob.setNumReduceTasks(6);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
