package org.macau.flickr.spatial.sjmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.spatial.sjmr.SJMRSpatialMapper;
import org.macau.flickr.spatial.sjmr.SJMRSpatialReducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SJMRSpatialSimilarityJoin {

public static boolean SJMRSpatialJoin(Configuration conf) throws Exception{
		
		Job spaitialJob = new Job(conf,"Spatial Similarity Join");
		spaitialJob.setJarByClass(SJMRSpatialSimilarityJoin.class);
		
		spaitialJob.setMapperClass(SJMRSpatialMapper.class);
		//there can add one combiner which can combine the result
		//spaitialJob.setCombinerClass(TemporalBasicReducer.class);
		
		spaitialJob.setReducerClass(SJMRSpatialReducer.class);
		
		spaitialJob.setMapOutputKeyClass(IntWritable.class);
		spaitialJob.setMapOutputValueClass(FlickrValue.class);
		
//		spaitialJob.setOutputKeyClass(Text.class);
//		spaitialJob.setOutputValueClass(Text.class);
		spaitialJob.setNumReduceTasks(6);
		
		FileInputFormat.addInputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		
		if(spaitialJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
