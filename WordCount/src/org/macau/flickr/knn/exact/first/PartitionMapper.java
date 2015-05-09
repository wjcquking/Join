package org.macau.flickr.knn.exact.first;
/**
 * author:
 * description:
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.spatial.Distance;

public class PartitionMapper extends
	Mapper<Object,Text,Text,Text>{
	
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();
	public static List<FlickrValue> pivotList = new ArrayList<FlickrValue>();
	
	
	/**
	 * the setup function load the pivot list from the DFS
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("setup");
		
		String path = kNNUtil.pivotOutputPath + "/flickr.kmeans.data";
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		String value = null;
		Path pathq = new Path(path);
		FSDataInputStream fsr = hdfs.open(pathq);
		BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8")); 
		
		int count = 0;
	    while ((value = bis.readLine()) != null) {
	    	//System.out.println(value);
	    	FlickrValue fv = new FlickrValue();
			long id =Long.parseLong(value.toString().split(";")[0]);
			double lat = Double.parseDouble(value.toString().split(";")[1]);
			double lon = Double.parseDouble(value.toString().split(";")[2]);
			long timestamp = Long.parseLong(value.toString().split(";")[3]);
			

			PartitionJob.R_Partition[count].setLat(lat);
			PartitionJob.R_Partition[count].setLon(lon);
			PartitionJob.S_Partition[count].setLat(lat);
			PartitionJob.S_Partition[count].setLon(lon);
			
			
			count++;
			fv.setId(id);
			fv.setLat(lat);
			fv.setLon(lon);
			fv.setTimestamp(timestamp);
			
			pivotList.add(new FlickrValue(fv));
	    }	
	}
	
	/**
	 *  
	 *  partition the universe into many partitions
	 */
	public void map(Object key,Text value,Context context)
		throws IOException, InterruptedException{
		
		InputSplit inputSplit = context.getInputSplit();
		//R: 0; S:1
		int tag;
		
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		if(fileName.contains(FlickrSimilarityUtil.R_TAG)){
			
			tag = 0;
			
		}else{
			tag = 1;
		}
		
		

		System.out.println(value);
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		
		double min  = 1000;
		int minPartition = 0;
		double dist =0;
		
//			System.out.println("ll" + pivotList.size());
		for(int i = 0; i < pivotList.size();i++){			
			
			dist =Distance.GreatCircleDistance(pivotList.get(i).getLat(), pivotList.get(i).getLon(), lat, lon); 
			
			
			if(i == 0){
				min = dist;
				minPartition = 0;
			}
			
			if(min > dist){
				min = dist;
				minPartition = i;
			}
			
//				System.out.println("i:"+i + "dist" + dist + "min " + min + "minPartition " + minPartition);
			
		}
		
		if(tag == FlickrSimilarityUtil.R_tag){
			
			if(PartitionJob.R_Partition[minPartition].getMinDistance() > dist){
				
				PartitionJob.R_Partition[minPartition].setMinDistance(dist);
				
			}
			
			if(PartitionJob.R_Partition[minPartition].getMaxDistance() < dist){
				
				PartitionJob.R_Partition[minPartition].setMaxDistance(dist);
				
			}
			PartitionJob.R_Partition[minPartition].setCount(PartitionJob.R_Partition[minPartition].getCount()+1);
			
		}else if(tag == FlickrSimilarityUtil.S_tag){
			
			if(PartitionJob.S_Partition[minPartition].getkNNDistance().size() < kNNUtil.k){
				
				PartitionJob.S_Partition[minPartition].getkNNDistance().add(new Double(dist));
				Collections.sort(PartitionJob.S_Partition[minPartition].getkNNDistance());
				
				//this code is not too good, I can change another one which record all the distance, then sort and delete.
			}else if (PartitionJob.S_Partition[minPartition].getkNNDistance().get(kNNUtil.k-1) >= dist){
				
				PartitionJob.S_Partition[minPartition].getkNNDistance().remove(kNNUtil.k -1);
				PartitionJob.S_Partition[minPartition].getkNNDistance().add(dist);
				Collections.sort(PartitionJob.S_Partition[minPartition].getkNNDistance());
				
			}
			
			if(PartitionJob.S_Partition[minPartition].getMinDistance() > dist){
				
				PartitionJob.S_Partition[minPartition].setMinDistance(dist);
				
			}
			
			if(PartitionJob.S_Partition[minPartition].getMaxDistance() < dist){
				
				PartitionJob.S_Partition[minPartition].setMaxDistance(dist);
				
			}
			
			PartitionJob.S_Partition[minPartition].setCount(PartitionJob.S_Partition[minPartition].getCount()+1);
			
		}
		
		outputValue.set(minPartition + ";" + tag + ";" + dist + ";"+id + ";"+ lat + ";" + lon);
		outputKey.set("");
		
		context.write(outputValue,outputKey);
	}

	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("clean up");
		
		
	}
}
