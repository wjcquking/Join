package org.macau.stjoin.ego.textual.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TextualJoinJob {
	
public static boolean TextualJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Textual EGO Join");
		basicJob.setJarByClass(TextualJoinJob.class);
		
		basicJob.setMapperClass(TextualJoinMapper.class);
		
		basicJob.setReducerClass(TextualJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(IntWritable.class);
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
