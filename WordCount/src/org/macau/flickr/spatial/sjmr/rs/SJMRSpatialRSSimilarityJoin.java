package org.macau.flickr.spatial.sjmr.rs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SJMRSpatialRSSimilarityJoin {

public static boolean SJMRSpatialJoin(Configuration conf) throws Exception{
		
		Job spaitialJob = new Job(conf,"Spatial RS Similarity Join");
		spaitialJob.setJarByClass(SJMRSpatialRSSimilarityJoin.class);
		
		spaitialJob.setMapperClass(SJMRSpatialRSMapper.class);
		//there can add one combiner which can combine the result
		//spaitialJob.setCombinerClass(TemporalBasicReducer.class);
		
		spaitialJob.setReducerClass(SJMRSpatialRSReducer.class);
		
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
