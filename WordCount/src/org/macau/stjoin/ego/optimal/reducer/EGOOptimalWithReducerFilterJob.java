package org.macau.stjoin.ego.optimal.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.FlickrValueWithCandidateTags;


/**
 * 
 * @author hadoop
 * 
 * In the original EGO optimal algorithm, we only consider the partition value in the mapper, we don't consider
 * the filter ability in the reducer
 * So I propose a new algorithm
 * In the Mapper, each record record the candidate tag in all the feature
 * In the reducer, for each pair, we first compare the feature candidate tag, for each feature, there must be 
 * a common candidate tag, then we can verify the result
 */
public class EGOOptimalWithReducerFilterJob {

	public static boolean EGOOptimalWithReducerFilterJoin(Configuration conf,int reducerNumber) throws Exception{
		
		Job basicJob = new Job(conf,"EGO optimal Join with Reducer filter");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(EGOOptimalWithReducerFilterMapper.class);
//		basicJob.setCombinerClass(TemporalJoinReducer.class);
		
		basicJob.setReducerClass(EGOOptimalWithReducerFilterReducer.class);
		
		basicJob.setMapOutputKeyClass(LongWritable.class);
		basicJob.setMapOutputValueClass(FlickrValueWithCandidateTags.class);
		
//		basicJob.setOutputKeyClass(Text.class);
//		basicJob.setOutputValueClass(Text.class);
		basicJob.setNumReduceTasks(reducerNumber);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
