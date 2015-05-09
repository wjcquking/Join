package org.macau.flickr.knn.exact.second;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.spatial.Distance;

public class kNNJoinFunction {

	/**
	 * 
	 * @param R_Partition
	 * @param S_Partition
	 * get the theta value for each partition in R.
	 */
	public static double boundingkNN(kNNPartition R_Partition,kNNPartition[] S_Partition){
			
		List<Double> topList = new ArrayList<Double>();
		
		for(int j = 0; j < S_Partition.length;j++){
			
			for(int k = 0; k < S_Partition[j].getkNNDistance().size();k++){
				
				double distance = Distance.GreatCircleDistance(R_Partition.getLat(),  R_Partition.getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
				double upperBound = R_Partition.getMaxDistance() + distance + S_Partition[j].getkNNDistance().get(k);
				
				if(topList.size() < kNNUtil.k){
					
					topList.add(upperBound);
					Collections.sort(topList);
					
				}else if(topList.get(kNNUtil.k-1) > upperBound){
					
					topList.remove(kNNUtil.k-1);
					topList.add(upperBound);
					Collections.sort(topList);
				}
						
			}
		}
		
		return topList.get(kNNUtil.k-1);
	
	}
	
	
	/**
	 * 
	 * @param R_Partition
	 * @param S_Partition
	 * get the theta value for each partition in R.
	 */
	public static double boundingkNN(kNNPartition R_Partition,List<kNNPartition> S_Partition){
			
		List<Double> topList = new ArrayList<Double>();
		
		for(int j = 0; j < S_Partition.size();j++){
			
			for(int k = 0; k < S_Partition.get(j).getkNNDistance().size();k++){
				
				double distance = Distance.GreatCircleDistance(R_Partition.getLat(),  R_Partition.getLon(),  S_Partition.get(j).getLat(), S_Partition.get(j).getLon());
				double upperBound = R_Partition.getMaxDistance() + distance + S_Partition.get(j).getkNNDistance().get(k);
				
				if(topList.size() < kNNUtil.k){
					
					topList.add(upperBound);
					Collections.sort(topList);
					
				}else if(topList.get(kNNUtil.k-1) > upperBound){
					
					topList.remove(kNNUtil.k-1);
					topList.add(upperBound);
					Collections.sort(topList);
				}
						
			}
		}
		
		return topList.get(kNNUtil.k-1);
	
	}
	
	/**
	 * 
	 * @param R_Partition: the R Partition Array.
	 * @param S_Partition: the S Partition Array.
	 * get the theta value for each partition in R.
	 */
	public static List<Double> boundingkNN(kNNPartition[] R_Partition,kNNPartition[] S_Partition){
		
		List<Double> thetaList = new ArrayList<Double>();
		
		for(int i = 0; i < R_Partition.length;i++){
			
			List<Double> topList = new ArrayList<Double>();
			
			for(int j = 0; j < S_Partition.length;j++){
				
				for(int k = 0; k < S_Partition[j].getkNNDistance().size();k++){
					
					double distance = Distance.GreatCircleDistance(R_Partition[i].getLat(),  R_Partition[i].getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
					double upperBound = R_Partition[i].getMaxDistance() + distance + S_Partition[j].getkNNDistance().get(k);
					
					if(topList.size() < kNNUtil.k){
						
						topList.add(upperBound);
						Collections.sort(topList);
						
					}else if(topList.get(kNNUtil.k-1) > upperBound){
						
						topList.remove(kNNUtil.k-1);
						topList.add(upperBound);
						Collections.sort(topList);
					}
							
				}
			}
			
			thetaList.add(topList.get(kNNUtil.k-1));
			topList.clear();
		}
		return thetaList;
	}
}
