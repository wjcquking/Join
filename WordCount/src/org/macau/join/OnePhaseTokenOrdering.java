package org.macau.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.join.OnePhaseTokenOrdering.TokenSumSortReducer.TokenCount;
import org.macau.util.SimilarityUtil;


public class OnePhaseTokenOrdering {
	
	
	/**
	 * 
	 * @param conf
	 * @return
	 * @throws Exception
	 * if the job finished, then return true
	 */
	public static boolean OnePhaseTokenOrder(Configuration conf) throws Exception{
		
		Job job = new Job(conf, "Token count and sort");
		job.setJarByClass(OnePhaseTokenOrdering.class);
		job.setMapperClass(org.macau.join.phase1.Map.class);
		job.setCombinerClass(TokenLocalSumCombiner.class);
		job.setReducerClass(TokenSumSortReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(job, new Path(SimilarityUtil.tokenOutputPath));
		
		if(job.waitForCompletion(true)){
			return true;
		}
		else 
			return false;
	}
	
	
	/**
	 * @param
	 * @author hadoop
	 * @version 1.0
	 * input:  KEY:token VALUE:1
	 * output: KEY:token VALUE:localCount
	 */
	public static class TokenLocalSumCombiner extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
	
			int sum = 0;
			for (IntWritable val : values) {
				System.out.println("Phase1: Combiner---key :" + key.toString() + " ,value : " + val.toString());
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	/**
	 * @param
	 * @author hadoop
	 * @version 1.0
	 * input: KEY:token VALUE:localCount
	 * output: KEY:Token VALUE:null
	 */
	public static class TokenSumSortReducer extends
		Reducer<Text, IntWritable, Text, Text> {
		
		public static class TokenCount implements Comparable<TokenCount>{
			private int count;
			private String token;
			
			public TokenCount(String token,int count){
				this.token = token;
				this.count = count;
			}
			
			public int getCount() {
				return count;
			}
			public void setCount(int count) {
				this.count = count;
			}
			public String getToken() {
				return token;
			}
			public void setToken(String token) {
				this.token = token;
			}
			
			
			@Override
			public int compareTo(TokenCount tokenCount) {
				int cop = count-tokenCount.getCount();        
				if (cop != 0)            
					return cop;        
				else            
					return token.compareTo(tokenCount.token); 
			}
		}
		
		private ArrayList<TokenCount> tokenCounts = new ArrayList<TokenCount>();
		private Context context;
		
		
		/**
		 * execute once when the reduce is finished
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Collections.sort(tokenCounts);
			
			for(TokenCount tokenCount : tokenCounts){
				context.write(new Text(tokenCount.token), null);
			}
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
	
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			this.setContext(context);
			
			//System.out.println("Phase1: reduce---key :" + key.toString() + ",value : " + sum);
			
			
			tokenCounts.add(new TokenCount(key.toString(),sum));
		}
		
		
		public Context getContext() {
			return context;
		}
		public void setContext(Context context) {
			this.context = context;
		}
	}
	
	
	//main function test the arraylist sort
	public static void main(String[] args){
		ArrayList<TokenCount> tokenCounts = new ArrayList<TokenCount>();
		tokenCounts.add(new TokenCount("g",10));
		tokenCounts.add(new TokenCount("1",1));
		tokenCounts.add(new TokenCount("kik",10));
		tokenCounts.add(new TokenCount("afda",2));
		
		Collections.sort(tokenCounts);
		for(TokenCount tokenCount : tokenCounts){
			System.out.println(tokenCount.token);
		}
		
		String pairs= "1,4	1";
		System.out.println(pairs.length() +" "+ pairs.charAt(3));
		for(int i = 0; i < pairs.length();i++){
			System.out.println(i+","+pairs.charAt(i));
		}
		System.out.println("hashcode:"+ pairs.hashCode());
		
		String[] result = pairs.split("\\s+");
		System.out.println(result.length +"  "+ result[0]);
		System.out.println("hashcode:"+ pairs.hashCode());
	}
}
