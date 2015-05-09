package org.macau.local.mfeat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.macau.local.analysis.time.ThreeFeatureJoin;

/**
 * 
 * @author mb25428
 * there are six features in the data
 */
public class ComputeOrder {

	public static double mFeatSelfJoin(List<List<Double>> list, double threshold){
		Long startTime = System.currentTimeMillis();
		
		double pairCount  = 0;
		double count = 0;
		for(int i = 0; i < list.size();i++){
			for(int j = i+1 ;j < list.size();j++){
				pairCount++;
//				System.out.println(MFeatUtil.getSimilarityValue(list.get(i), list.get(j)));
				if(MFeatUtil.getSimilarityValue(list.get(i), list.get(j))  < threshold){
					count++;
				}
			}
		}
		
		double selectivity = count /pairCount;
//		System.out.println(selectivity);
		System.out.println((System.currentTimeMillis() -startTime)/ (float) 1000.0);
		return selectivity;
	}
	
	public static double mFeatSelfJoinOrder(List<List<Double>> list, double threshold){
		String order = "fou:fac:kar:pix:zer:mor";
		String [] orderArray = order.split(":");
		
		List<String> orderList= new ArrayList<String>();
		
		for(String str : orderArray){
			orderList.add("D:\\Data\\mfeat\\mfeat-" + str);
		}
		
		int[] intArray = {3,6,5,1,2,4};
//		int[] intArray = {4,2,1,3,5,6};
		
		List<List<Double>> fouList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatFou);
		List<List<Double>> facList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatFac);
		List<List<Double>> karList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatKar);
		List<List<Double>> pixList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatPix);
		List<List<Double>> zerList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatZer);
		List<List<Double>> morList = ReadMFeatData.readFileByLines(MFeatUtil.mFeatMor);
		
		Map<Integer,List<List<Double>>> listMap = new HashMap<Integer,List<List<Double>>>();
		listMap.put(1, fouList);
		listMap.put(2, facList);
		listMap.put(3, karList);
		listMap.put(4, pixList);
		listMap.put(5, zerList);
		listMap.put(6, morList);
		
		Map<Integer,Double> thresholdMap = new HashMap<Integer,Double>();
		thresholdMap.put(1, MFeatUtil.fouThreshold);
		thresholdMap.put(2, MFeatUtil.facThreshold);
		thresholdMap.put(3, MFeatUtil.karThreshold);
		thresholdMap.put(4, MFeatUtil.pixThreshold);
		thresholdMap.put(5, MFeatUtil.zerThreshold);
		thresholdMap.put(6, MFeatUtil.morThreshold);
		
		Long startTime = System.currentTimeMillis();
		
		double pairCount  = 0;
		double count = 0;
		for(int i = 0; i < fouList.size();i++){
//			System.out.println(i);
			for(int j = i+1 ;j < fouList.size();j++){
				pairCount++;
				
				int featureCount = 0;
				for(int m : intArray){
					if(MFeatUtil.getSimilarityValue(listMap.get(m).get(i), listMap.get(m).get(j))  < thresholdMap.get(m)){
						featureCount++;
						if(featureCount== 6){
							count++;
						}
					}else{
						break;
					}
				}
			}
		}
		
		double selectivity = count /pairCount;
//		System.out.println(selectivity);
		System.out.println((System.currentTimeMillis() -startTime)/ (float) 1000.0);
		return selectivity;
	}
	public static void main(String[] args){


		mFeatSelfJoinOrder(ReadMFeatData.readFileByLines(MFeatUtil.mFeatFou),MFeatUtil.fouThreshold);
//		mFeatSelfJoin(ReadMFeatData.readFileByLines(MFeatUtil.mFeatFac),MFeatUtil.facThreshold);
//		mFeatSelfJoin(ReadMFeatData.readFileByLines(MFeatUtil.mFeatKar),MFeatUtil.karThreshold);
//		mFeatSelfJoin(ReadMFeatData.readFileByLines(MFeatUtil.mFeatPix),MFeatUtil.pixThreshold);
//		mFeatSelfJoin(ReadMFeatData.readFileByLines(MFeatUtil.mFeatZer),MFeatUtil.zerThreshold);
//		mFeatSelfJoin(ReadMFeatData.readFileByLines(MFeatUtil.mFeatMor),MFeatUtil.morThreshold);
		
		

	}
}
