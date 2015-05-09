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
import org.macau.token.LoadPairs;
import org.macau.util.SimilarityUtil;

public class OnePhaseRecordJoin {

public static boolean OnePahseRecordJoinJob(Configuration conf) throws Exception{
		
		Job secondReadJob = new Job(conf,"one Pahse record join");
		secondReadJob.setJarByClass(OnePhaseRecordJoin.class);
		
		secondReadJob.setMapperClass(ReadParisDataMapper.class);
		//secondReadJob.setCombinerClass(IntSumReducer.class);
		secondReadJob.setReducerClass(CombineReducer.class);
		
		
		secondReadJob.setOutputKeyClass(Text.class);
		secondReadJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(secondReadJob, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(secondReadJob, new Path(SimilarityUtil.finalPairsOutputPath));
		
		if(secondReadJob.waitForCompletion(true))
			return true;
		else
			return false;
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
	
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//System.out.println("phase 3: map input---key :" + key.toString() + ",value : " + value.toString());
			
			
			String line = value.toString().toLowerCase(); // 全部转为小写字母
			
			int id = Integer.parseInt(line.split(":")[0]);
			
			
			Set pairSet;
			try {
				if(LoadPairs.getPairSet(id+"") != null){
					pairSet = LoadPairs.getPairSet(id+"");
					
					Iterator iterator = pairSet.iterator();
					while (iterator.hasNext()){
					   String pair = (String) iterator.next();
					   String[] pairArray = pair.split("%");
					   //System.out.println("pair"+pair);
					   String outputKey = "";
					   
					   //change the pairs to the some id
					   if(id <= Integer.parseInt(pairArray[0]))
						   outputKey = id +"," + pairArray[0];
					   else{
						   outputKey = pairArray[0]+","+ id;
					   }
					   
					   String outputValue = line + "~" + pairArray[1];
					   context.write(new Text(outputKey), new Text(outputValue));
					   //System.out.println("phase 3: map output key:"+outputKey + "outputValue:" + outputValue);
					}
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
			
			List<String> list = new ArrayList<String>();
			
			for (Text val : values) {
				list.add(val.toString());
			}
			
			//there will be two elements in the list
			String R1 = list.get(0).split("~")[0];
			String sim = list.get(0).split("~")[1];
			String R2 = list.get(1).split("~")[0];
			
			//System.out.println("phase 3: reduce--ouput--key :"+R1+ "," + sim + "," + R2);
			
			context.write(new Text(R1+ "," + sim + "," + R2), null);
		}
	}
}
