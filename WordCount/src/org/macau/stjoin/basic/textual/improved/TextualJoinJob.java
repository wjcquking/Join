package org.macau.stjoin.basic.textual.improved;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValueWithMultiFeature;

public class TextualJoinJob {
	
public static boolean TextualSimilarityBasicJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"Textual Basic Similarity Join");
		basicJob.setJarByClass(TextualJoinJob.class);
		
		basicJob.setMapperClass(TextualJoinMapper.class);
		
		basicJob.setReducerClass(TextualJoinReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
		basicJob.setMapOutputValueClass(FlickrValueWithMultiFeature.class);
		
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}

}
