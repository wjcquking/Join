package org.macau.flickr.knn.exact.second;

/**
 * Simple MapReduce Job which can find the range of the spatial data
 * and the number of each location point
 * for the paris flickr data, I analyze the longitude and latitude range
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.knn.exact.first.PartitionJob;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrSimilarityUtil;


public class kNNJoinJob {

	public static kNNPartition[] R_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	public static kNNPartition[] S_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	
	
	public static boolean spatialPartitionjob(Configuration conf) throws Exception{
		
		
		Job job = new Job(conf,"kNN exact Join Job");
		
		job.setJarByClass(kNNJoinJob.class);
		
		Path r_Path = new Path(kNNUtil.R_InformationPart);
		Path s_Path = new Path(kNNUtil.S_InformationPart);
		
		Configuration conf1 = new Configuration();
		conf1.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf1);
		
		FileStatus r_Stats[] = hdfs.listStatus(r_Path);
		FileStatus s_Stats[] = hdfs.listStatus(s_Path);
		
		String value = null;
		
		for(int i = 0; i < r_Stats.length;i++){
			Path pathq = r_Stats[i].getPath();
			
			FSDataInputStream fsr = hdfs.open(pathq);
			
			BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8")); 
			
			
			while ((value = bis.readLine()) != null) {
				//The R partition:pid + ";" + lat + ";" + lon + ";" + count + ";" + minDistance + ";" + maxDistance
				int pid =Integer.parseInt(value.toString().split(";")[0]);
				double lat = Double.parseDouble(value.toString().split(";")[1]);
				double lon = Double.parseDouble(value.toString().split(";")[2]);
				int count = Integer.parseInt(value.toString().split(";")[3]);
				double minDistance = Double.parseDouble(value.toString().split(";")[4]);
				double maxDistance = Double.parseDouble(value.toString().split(";")[5]);
				
				R_Partition[pid] = new kNNPartition(pid,lat,lon,count,minDistance, maxDistance);
				
			}
			
		}
		
		for(int i = 0; i < s_Stats.length;i++){
			Path pathq = s_Stats[i].getPath();
			
			FSDataInputStream fsr = hdfs.open(pathq);
			
			BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8")); 
			
			
			while ((value = bis.readLine()) != null) {
				//The R partition:pid + ";" + lat + ";" + lon + ";" + count + ";" + minDistance + ";" + maxDistance
				int pid =Integer.parseInt(value.toString().split(";")[0]);
				double lat = Double.parseDouble(value.toString().split(";")[1]);
				double lon = Double.parseDouble(value.toString().split(";")[2]);
				int count = Integer.parseInt(value.toString().split(";")[3]);
				double minDistance = Double.parseDouble(value.toString().split(";")[4]);
				double maxDistance = Double.parseDouble(value.toString().split(";")[5]);
				
				List<Double> kNNDistance = new ArrayList<Double>();
				
				for(String s : value.toString().split(";")[6].split(",")){
					kNNDistance.add(Double.parseDouble(s));
				}
				
				S_Partition[pid] = new kNNPartition(pid,lat,lon,count,minDistance, maxDistance);
				S_Partition[pid].setkNNDistance(kNNDistance);
			}
			
		}
		
		
		
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			
//			R_Partition[i] = new kNNPartition(PartitionJob.R_Partition[i]);
			
//			S_Partition[i] = new kNNPartition(PartitionJob.S_Partition[i]);
		}
		
		int R_Sum = 0;
		int S_Sum = 0;
		for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
			R_Sum+= R_Partition[i].getCount();
			S_Sum += S_Partition[i].getCount();
			System.out.println(S_Partition[i].getLat() + ";"+ S_Partition[i].getLon()+ ";" + R_Partition[i].getLat() + ";"+R_Partition[i].getLon());
//			System.out.println(i + ":" + R_Partition[i].getCount()+","+ R_Partition[i].getMinDistance()+","+ R_Partition[i].getMaxDistance() + ";" + S_Partition[i].getCount()+","+ S_Partition[i].getMinDistance()+","+ S_Partition[i].getMaxDistance());
		}
		
		System.out.println("R:" + R_Sum + ";S:" + S_Sum);
		
		job.setMapperClass(kNNJoinMapper.class);
//		job.setCombinerClass(PartitionReducer.class);
		job.setReducerClass(kNNJoinReducer.class);
//		job.setNumReduceTasks(0);
		
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FlickrPartitionValue.class);
		
		FileInputFormat.addInputPath(job, new Path(FlickrSimilarityUtil.flickrOutputPath));
		FileOutputFormat.setOutputPath(job, new Path(FlickrSimilarityUtil.flickrResultPath));
		
		if(job.waitForCompletion(true)){
			
			
			return true;
		}
		else
			return false;
	}
}
