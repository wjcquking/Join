package org.macau.join;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.token.TokenGenerate;
import org.macau.util.SystemUtil;

/**
 * 
 * @author hadoop
 * read the original data and preprocess the data
 * input line data
 * output word,count(1)
 * 
 */
public class ReadRawDataMapper extends
	Mapper<Object, Text, Text, IntWritable>  {
	
	private final static IntWritable one = new IntWritable(1);
	
	private Text word = new Text();
	/**
	 * TokenizerMapper
	 * get the text tokens from the raw data
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//System.out.println("phase 1: map---key :" + key.toString() + ",value : " + value.toString());
		
		//change the string to lower case
		String line = value.toString().toLowerCase(); 

		
		//for the dblp data
		String[] lineArray = line.split(":");
		
		
		String text = lineArray[1]+ " " + lineArray[2];

		SystemUtil.clean(text);
		StringTokenizer itr = new StringTokenizer(text);
	    while (itr.hasMoreTokens()) {
	        word.set(itr.nextToken());
	        context.write(word, one);
	    }
	}
}
