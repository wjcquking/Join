package org.macau.flickr.spatial.analysis;

/**
 * Simple MapReduce Job which can find the range of the spatial data
 * and the number of each location point
 * for the paris flickr data, I analyze the longitude and latitude range
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;


public class SpatialAccount {

	public static boolean spatialAccountJob(Configuration conf) throws Exception{
		
		Job accountJob = new Job(conf,"Spatial Account Job");
		
		accountJob.setJarByClass(SpatialAccount.class);
		
		accountJob.setMapperClass(SpatialAccountMapper.class);
		accountJob.setCombinerClass(SpatialAccountReducer.class);
		accountJob.setReducerClass(SpatialAccountReducer.class);
		
		accountJob.setOutputKeyClass(DoubleWritable.class);
		accountJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(accountJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(accountJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(accountJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
