package org.macau.flickr.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.temporal.ReadTemporalDataMapper;
import org.macau.flickr.temporal.TemporalBasicReducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TemporalSimilarityJoin {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf) throws Exception{
		
		Job secondReadJob = new Job(conf,"Temporal Similarity Join");
		secondReadJob.setJarByClass(TemporalSimilarityJoin.class);
		
		secondReadJob.setMapperClass(ReadTemporalDataMapper.class);
		
		
		//there can add one combiner which can combine the result
		//secondReadJob.setCombinerClass(TemporalBasicReducer.class);
		
		secondReadJob.setReducerClass(TemporalBasicReducer.class);
		
		secondReadJob.setMapOutputKeyClass(LongWritable.class);
		secondReadJob.setMapOutputValueClass(FlickrValue.class);
		
//		secondReadJob.setOutputKeyClass(Text.class);
//		secondReadJob.setOutputValueClass(Text.class);
//		secondReadJob.setNumReduceTasks(6);
		
		FileInputFormat.addInputPath(secondReadJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(secondReadJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(secondReadJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
