package org.macau.stjoin.ego.spatial.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;


/**
 * 
 * @author hadoop
 * Word Count
 * Calculate the count number for each timeinterval
 * Modify Date: 2015-01-17
 */
public class SpatialCountJob {

	public static boolean SpatialCount(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Count Job");
		basicJob.setJarByClass(SpatialCountJob.class);
		
		basicJob.setMapperClass(SpatialCountMapper.class);
//		basicJob.setCombinerClass(SpatialCountReducer.class);
		
		basicJob.setReducerClass(SpatialCountReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
		basicJob.setMapOutputValueClass(LongWritable.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
