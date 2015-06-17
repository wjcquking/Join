package org.macau.stjoin.ego.basic.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;


/**
 * 
 * @author hadoop
 * EGO Optimal Join
 * Find the min max reducer time partition solution to do the join operation
 * because we find the optimal partition solution
 * 
 */
public class EGOSortJob {

	public static boolean EGOSort(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"EGO optimal Join");
		basicJob.setJarByClass(EGOSortJob.class);
		
		basicJob.setMapperClass(EGOSortMapper.class);
		
		basicJob.setReducerClass(EGOSortReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
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
