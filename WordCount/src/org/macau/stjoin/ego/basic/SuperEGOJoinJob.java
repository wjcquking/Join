package org.macau.stjoin.ego.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;


/****************************************************
 * 
 * @author hadoop
 * Map: Input:  KEY  : 
 *              Value:
 *      output: KEY  :
 *      		Value:
 *      
 * Reduce: Input: KEY  :
 * 				  Value:
 * 
 * 
 * Description: 
 * According to the paper "Super-EGO Fast Multidimensional Similarity Join"
 * Change the algorithm to the MapReduce Format and be suitable for multi-dimension join
 * 
 * 
 * 
 * Date: 2014-12-29
 ****************************************************/
public class SuperEGOJoinJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Super EGO Join Job");
		basicJob.setJarByClass(SuperEGOJoinJob.class);
		
		basicJob.setMapperClass(SuperEGOJoinMapper.class);
//		basicJob.setCombinerClass(TemporalJoinReducer.class);
		
		basicJob.setReducerClass(SuperEGOJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
		basicJob.setMapOutputValueClass(FlickrValue.class);
		
//		basicJob.setOutputKeyClass(Text.class);
//		basicJob.setOutputValueClass(Text.class);
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrResultPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
