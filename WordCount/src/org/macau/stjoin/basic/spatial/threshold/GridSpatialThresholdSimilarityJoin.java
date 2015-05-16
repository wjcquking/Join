package org.macau.stjoin.basic.spatial.threshold;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class GridSpatialThresholdSimilarityJoin {

public static boolean GridSpatialJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job spaitialJob = new Job(conf,"Spatial RS Similarity Join");
		spaitialJob.setJarByClass(GridSpatialThresholdSimilarityJoin.class);
		
		spaitialJob.setMapperClass(GridSpatialThresholdMapper.class);
		spaitialJob.setReducerClass(GridSpatialThresholdReducer.class);
		
		spaitialJob.setMapOutputKeyClass(Text.class);
		spaitialJob.setMapOutputValueClass(FlickrValue.class);
		
		spaitialJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(spaitialJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		
		if(spaitialJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
