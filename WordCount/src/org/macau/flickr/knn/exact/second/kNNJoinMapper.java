package org.macau.flickr.knn.exact.second;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.spatial.Distance;


public class kNNJoinMapper extends
	Mapper<Object,Text,IntWritable,FlickrPartitionValue>{
	
	private final IntWritable outputKey = new IntWritable();
	private final FlickrPartitionValue outputValue = new FlickrPartitionValue();
	public static List<FlickrValue> pivotList = new ArrayList<FlickrValue>();
	
	public static double[][] LBArray = new double[kNNUtil.REDUCER_NUMBER][kNNUtil.REDUCER_NUMBER];
	
	public static int[] count = new int[kNNUtil.REDUCER_NUMBER]; 

	
	/**
	 * compute the Lower bound for each Partition of S
	 */
	public static void compuateLBOfReplica(kNNPartition[] R_Partition,kNNPartition[] S_Partition,List<Double> thetaList){
		
		for(int j = 0; j < kNNUtil.REDUCER_NUMBER;j++){
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
				
//				System.out.println(S_Partition[j].getLat() + ";"+ S_Partition[j].getLon()+ ";" + R_Partition[i].getLat() + ";"+R_Partition[i].getLon());
				double distance = Distance.GreatCircleDistance(R_Partition[i].getLat(),  R_Partition[i].getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
				
				LBArray[j][i]= distance - R_Partition[i].getMaxDistance() - thetaList.get(i);
				
//				System.out.println(j + ":" + i + "{" + LBArray[j][i]);
//				System.out.println("Start:" + j + "," + i + ";"+ distance + ":" + R_Partition[i].getMaxDistance() + ":"+thetaList.get(i) +":"+ LBArray[j][i]);
						
			}
		}
	}
	
	/**
	 * the setup function complete LB of Replicas
	 * may want to load the Lower bound for each partition
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("setup");
		// compute the Lower Bound of replication
		List<Double> thetaList = kNNJoinFunction.boundingkNN(kNNJoinJob.R_Partition, kNNJoinJob.S_Partition);

		for(double d : thetaList){
			System.out.println("d"+d);
		}
		compuateLBOfReplica(kNNJoinJob.R_Partition,kNNJoinJob.S_Partition,thetaList);
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			count[i] = 0;
		}
//		
//		int i = 0;
//		for(double d: thetaList){
//			System.out.println(i++ + ";"+d);
//		}
	}
	
	public void map(Object key,Text value,Context context)
		throws IOException, InterruptedException{
		
		FlickrPartitionValue fpv = new FlickrPartitionValue();
		int pid = Integer.parseInt(value.toString().split(";")[0]);
		
		//R: 0; S:1
		int dataset = Integer.parseInt(value.toString().split(";")[1]);
		double distance  = Double.parseDouble(value.toString().split(";")[2]);
		long id =Long.parseLong(value.toString().split(";")[3].trim());
		double lat = Double.parseDouble(value.toString().split(";")[4]);
		double lon = Double.parseDouble(value.toString().split(";")[5].trim());
		
		fpv.setPid(pid);
		fpv.setDataset(dataset);
		fpv.setDistance(distance);
		fpv.setId(id);
		fpv.setLat(lat);
		fpv.setLon(lon);
		
		
		outputValue.setPid(pid);
		outputValue.setDataset(dataset);
		outputValue.setDistance(distance);
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		
		
		//R
		if(dataset == FlickrSimilarityUtil.R_tag){
			outputKey.set(pid);
			
			context.write(outputKey,outputValue);
			
		}else if (dataset == FlickrSimilarityUtil.S_tag){
			
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
//				System.out.println(pid + "{" + i + "}"+LBArray[pid][i] + "Distance : " + distance);
				if(LBArray[pid][i] < distance){
					count[i]++;
//					System.out.println(pid + ":" + i + ":"+LBArray[pid][i] + "Distance : " + distance);
					outputKey.set(i);
					context.write(outputKey,outputValue);
				}
			}
		}
//		System.out.println(outputKey + ":" + outputValue);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("kNN join Map clean up");
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			System.out.println(i + ":" + count[i]);
		}
		
	}
}
