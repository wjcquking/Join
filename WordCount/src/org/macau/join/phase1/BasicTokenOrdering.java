/**
 * @author hadoop
 * the phase1
 * basic token ordering
 * 
 */
package org.macau.join.phase1;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.macau.util.SimilarityUtil;

/**
 * 
 * @author hadoop
 *
 */

public class BasicTokenOrdering {

	/**
	 * 
	 * @param conf
	 * @return if the sort job finished, return true, else return false
	 * @throws Exception
	 */
	public static boolean BasicTokenOrder(Configuration conf) throws Exception{
		//phase1
		Job tokenCountJob = new Job(conf, "Token count");
		System.out.println("Basic Token Ordering phase1 starts");
		TokenCount(tokenCountJob);
		
		//phase2
		Job sortJob = new Job(conf,"token Sort");
		
		if(tokenCountJob.waitForCompletion(true)){
			System.out.println("Basic Token Ordering phase1 finishs");
			System.out.println("Basic Token Ordering phase2 starts");
			TokenSort(sortJob);
			System.out.println("Basic Token Ordering phase2 finishs");
		}
		
		if(sortJob.waitForCompletion(true)){
			return true;
		}
		else 
			return false;
		
	}
	
	
	/**
	 * 
	 * @param job
	 * read the raw data
	 * output the Token and the count of the Token to the template directory
	 * inputPath:recordsGeneratePath
	 * outputPath:tokenPhase1Path
	 */
	public static void TokenCount(Job job) throws IOException{
		
		job.setJarByClass(BasicTokenOrdering.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(TokenSumReducer.class);
		job.setReducerClass(TokenSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(job, new Path(SimilarityUtil.tokenPhase1Path));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	}
	
	/**
	 * 
	 * @param sortJob
	 * @param tempDir
	 * @throws IOException
	 * sort the token according to the count number 
	 */
	public static void TokenSort(Job sortJob) throws IOException{
		sortJob.setJarByClass(BasicTokenOrdering.class);
		
		FileInputFormat.addInputPath(sortJob,new Path(SimilarityUtil.tokenPhase1Path));
		
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		
		/*
		 * InverseMapper inverse the key and the value
		 * hadoop sort the key ascending
		 * */
        sortJob.setMapperClass(InverseMapper.class);
        sortJob.setReducerClass(TokenSortReducer.class);
        
        sortJob.setNumReduceTasks(1); 
        
        
        FileOutputFormat.setOutputPath(sortJob, new Path(SimilarityUtil.tokenOutputPath));
        
        sortJob.setMapOutputKeyClass(IntWritable.class);

	}
	
	/**
	 * @param
	 * @author hadoop
	 * @version 1.0
	 * input: Token localCount
	 * output: Token totalCount
	 */
	public static class TokenSumReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/**
	 * @author hadoop
	 * @param
	 * only one reduce task; with the total count as the key and only one reduce task,
     * tokens will end up being totally sorted by token count
     * */
	public static class TokenSortReducer extends
		Reducer<IntWritable,Text,Text,NullWritable>{
		
			private final NullWritable nullWritable = NullWritable.get();
			
			public void reduce(IntWritable key,Iterable<Text> values,
					Context context) throws IOException, InterruptedException{
				
				for(Text text: values){
					context.write(text, nullWritable);
				}
		}
	}
	
	
}
