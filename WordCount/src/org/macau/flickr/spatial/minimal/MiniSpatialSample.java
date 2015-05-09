package org.macau.flickr.spatial.minimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class MiniSpatialSample {

public static boolean MiniSpatial(Configuration conf) throws Exception{
		
		Job spaitialJob = new Job(conf,"Spatial Mini Sample Join");
		spaitialJob.setJarByClass(MiniSpatialSample.class);
		
		spaitialJob.setMapperClass(MiniSpatialSampleMapper.class);
		//there can add one combiner which can combine the result
		//spaitialJob.setCombinerClass(TemporalBasicReducer.class);
		
		spaitialJob.setReducerClass(MiniSpatialSampleReducer.class);
		
		spaitialJob.setMapOutputKeyClass(IntWritable.class);
		spaitialJob.setMapOutputValueClass(FlickrValue.class);
		
//		spaitialJob.setOutputKeyClass(Text.class);
//		spaitialJob.setOutputValueClass(Text.class);
//		spaitialJob.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		
		if(spaitialJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
