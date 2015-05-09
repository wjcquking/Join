package org.macau.join.phase3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.join.phase2.BasicKernel;
import org.macau.token.LoadPairs;
import org.macau.util.SimilarityUtil;


public class BasicRecordJoin {

	
	public static boolean BasicRecordJoinJob(Configuration conf) throws Exception{
		
		Job basicRecordJob = new Job(conf,"Final Result");
		basicRecordJob.setJarByClass(BasicKernel.class);
		
		basicRecordJob.setMapperClass(ReadParisDataMapper.class);
		//basicRecordJob.setCombinerClass(IntSumReducer.class);
		basicRecordJob.setReducerClass(CombineReducer.class);
		
		
		basicRecordJob.setOutputKeyClass(Text.class);
		basicRecordJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(basicRecordJob, new Path(SimilarityUtil.recordsInputPath));
		FileOutputFormat.setOutputPath(basicRecordJob, new Path(SimilarityUtil.ridPairsOutputPath));
		
		return basicRecordJob.waitForCompletion(true);
		
	}

	/**
	 * 
	 * @author hadoop
	 * read the original data and reorder the data
	 * Input: KEY: null VALUE:Text
	 * output:KEY: TEXT VALUE:Text(id+text)
	 */
	public static class ReadParisDataMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//System.out.println("phase 3: map input---key :" + key.toString() + ",value : " + value.toString());
			
			
			String line = value.toString().toLowerCase(); // 全部转为小写字母
			
			int id = line.hashCode();
			
			Set pairSet;
			try {
				pairSet = LoadPairs.getPairSet(id+"");
				
				Iterator iterator = pairSet.iterator();
				
				while (iterator.hasNext()){
				   String pair = (String) iterator.next();
				   String[] pairArray = pair.split("%");
				   //System.out.println("pair"+pair);
				   outputKey.set(id +"," + pairArray[0]);
				   outputValue.set(line + "~" + pairArray[1]);
				   context.write(outputKey, outputValue);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 
	 * @author hadoop
	 * INPUT: KEY:VALLUE:
	 * OUTPUT:KEY:VALUE:
	 */
	public static class CombineReducer extends
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
		
			System.out.println("Phase3: start reduce");
			//record all the objects which has the same prefix value
			List<String> list = new ArrayList<String>();
			
			for (Text val : values) {
				//System.out.println("phase 3: reduce---key :" + key.toString() + ",value : " + val.toString());
				
				list.add(val.toString());
			}
			
			//there will be two elements in the list
			String R1 = list.get(0).split("~")[0];
			String sim = list.get(0).split("~")[0];
			String R2 = list.get(1).split("~")[0];
			//System.out.println(R1+ "," + sim + "," + R2);
			context.write(new Text(R1+ "," + sim + "," + R2), null);
		}
	}
}
