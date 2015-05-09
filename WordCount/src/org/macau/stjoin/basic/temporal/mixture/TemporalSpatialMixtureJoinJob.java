package org.macau.stjoin.basic.temporal.mixture;

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
 *      output: KEY  : Use Two Features Value as the Key: temporal and spatial
 *      		Value: The Record
 *      
 * Reduce: Input: KEY  :
 * 				  Value:
 * 
 * Date: 2014-11-19
 * 
 **************************************************/


public class TemporalSpatialMixtureJoinJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Spatial Mixture Similarity Join");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(TemporalSpatialMixtureJoinMapper.class);
		
		basicJob.setReducerClass(TemporalSpatialMixtureJoinReducer.class);
		
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
