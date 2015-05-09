package org.macau.local.sample.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.macau.local.file.ReadFlickrData;
import org.macau.local.sample.SpatialBlackBoxWR2;
import org.macau.local.sample.SpatialSample;
import org.macau.local.sample.TemporalSample;
import org.macau.local.sample.TextualSample;
import org.macau.local.util.FlickrDataLocalUtil;

/**
 * 
 * @author mb25428
 * This class is to get the histogram of the two data set and 
 * 
 */


public class TemporalPartition {
	public static void main(String[] args){
		System.out.println("The Temporal Partition");
//		verifyHistogramWithSelectivity();
		getTheOptimalSolution();
	}
	
	/**
	 * This function is to verify that the result of histogram is same to 
	 * the candidate selectivity result
	 * 
	 * R * S * rho = r1*s1 + r2*s2 + ... + rn*sn
	 * 
	 * 
	 * Using the Temporal feature do the test and find that the result is meet the
	 * assumption
	 */
	public static void verifyHistogramWithSelectivity(){
		System.out.println("read the histogram");
		
		Map<Integer,Integer> rAccountMap = SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.rDataPath);
		Map<Integer,Integer> sAccountMap = SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Iterator iter = rAccountMap.entrySet().iterator(); 
		double sum = 0;
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    Object key =  entry.getKey();
		    Integer val = (Integer) entry.getValue();
		    if(sAccountMap.get(key) != null){
		    	sum += val * sAccountMap.get(key);
		    }
		}		
		System.out.println(sum);
		System.out.println("the selectivity is " + sum / 10000 / 10000);
		
	}
	
	
	public static void getTheOptimalSolution(){
		System.out.println("read the histogram");
		
		Map<Integer,Integer> rTemporalMap = SpatialBlackBoxWR2.getRTemporalWeightedData(FlickrDataLocalUtil.rDataPath);
		Map<Integer,Integer> sTemporalMap = SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.sDataPath);
		
		
		int r = 1000;
		int sSize = 10000;
		
		double temporalSelectivity = TemporalSample.temporalOlkenSample(ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,1),1000,SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.sDataPath),10000);;
		double spatialSelectivity =SpatialSample.spatialOlkenSample(ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,1),1000,SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.sDataPath),10000);;
		double textualSelectivity = TextualSample.textualOlkenSample(ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,1),r,SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath),sSize);;
		
		
		Map<Integer,Integer> rSpatialMap = SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.rDataPath);
		Map<Integer,Integer> sSpatialMap = SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Map<Integer,Integer> rTextualMap= SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.rDataPath);
		Map<Integer,Integer> sTextualMap= SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Iterator iter = rTemporalMap.entrySet().iterator(); 
		double sum = 0;
		double rSum = 0;
		double max = 0;
		List<PartitionAccount> partitionList = new ArrayList<PartitionAccount>();
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    Object key =  entry.getKey();
		    Integer val = (Integer) entry.getValue();
		    rSum += val;
		    if(sTemporalMap.get(key) != null){
		    	double product = val * sTemporalMap.get(key);
		    	
		    	partitionList.add(new PartitionAccount(val,sTemporalMap.get(key),product));
		    	
		    	if(product > max){
		    		max = product;
		    	}
		    	sum += product;
		    }
		}		
		System.out.println(sum);
		double earn = max - max*spatialSelectivity;
		System.out.println(earn);
		System.out.println("the selectivity is " + sum / rSum / rSum);
		
		System.out.println("The Temporal Selectivity is " + temporalSelectivity);
		System.out.println("The spatial Selectivity is " + spatialSelectivity);
		System.out.println("The textual Selectivity is " + textualSelectivity);
		
		ComparatorPartitionAccount comparator = new ComparatorPartitionAccount();
		Collections.sort(partitionList, comparator);
		
		double rPartitionSum = 0;
		double sPartitionSum = 0;
		
		double partitionSum = 0;
		double MaxearnValue = 0;
		
		for(PartitionAccount pl : partitionList){
//			System.out.println(pl.getProduct() * (1 - spatialSelectivity)/(pl.getrAccount()+ pl.getsAccount()) );
			
			partitionSum += pl.getProduct();
			rPartitionSum += pl.getrAccount();
			sPartitionSum += pl.getsAccount();
			
			double earnValue = partitionSum - rPartitionSum * sPartitionSum * spatialSelectivity;
			
			System.out.println(earnValue + "   " + earnValue/(rPartitionSum + sPartitionSum) + "  " + pl.getProduct()/( pl.getrAccount() +  pl.getsAccount()) );
			
		}
		
		System.out.println(partitionList.size());
		for(int i = 0; i < partitionList.size();i++){
			
		}
		
		
		
	}
	
	
}
