package org.macau.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
	//定义常量“1”
    private final static IntWritable one = new IntWritable(1);
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      System.out.println(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        System.out.println(word.toString());
        context.write(word, one);
      }
    }
  }
  
  //caculate the sum of each word
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    //values are the set of each node or file 
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
      int sum = 0;
      for (IntWritable val : values) {
    	  System.out.println(key + " "+val);
        sum += val.get();
      }
      
      System.out.println("sum is" + sum);
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
	  System.err.println("Usage: wordcount <in> <out>");
	  System.exit(2);
	}
	
	Job job = new Job(conf, "word count");
	job.setJarByClass(WordCount.class);
	job.setJobName("word count test");
	
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);//combiner use the same method as reducer
	job.setReducerClass(IntSumReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	
	
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	
    
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
