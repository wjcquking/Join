package org.macau.flickr.spatial.grid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class GridSpatialSimilarityJoin {

public static boolean GridSpatialJoin(Configuration conf) throws Exception{
		
		Job spaitialJob = new Job(conf,"Spatial RS Similarity Join");
		spaitialJob.setJarByClass(GridSpatialSimilarityJoin.class);
		
		spaitialJob.setMapperClass(GridSpatialMapper.class);
		spaitialJob.setReducerClass(GridSpatialReducer.class);
		
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
