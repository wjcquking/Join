package org.macau.local.sample;

/**
 * This file is for the Spatial sampling
 * I use the number of the tiles the point locate in and the neighbor tiles as the weight
 */

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.local.util.WeightData;

public class SpatialSampleWR {

	public final static double LatThreshold = 0.000001;

	public static void main(String[] args) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,FlickrDataLocalUtil.samplingProbablity);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		// get the s record Account
		
		Map<Double,Integer> accountMap = new HashMap<Double,Integer>();
		List<WeightData> weightDataList = new ArrayList<WeightData>();
		
		for(FlickrData record: sRecords){
			
			
			Double key = record.getLat();
			if(accountMap.get(key) == null){
				accountMap.put(key, new Integer(1));
			}else{
				int count = accountMap.get(key) + 1;
				accountMap.remove(key);
				accountMap.put(key, count);
			}
		}
		
		

		Iterator iter = accountMap.entrySet().iterator(); 
		
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    Object key = entry.getKey(); 
		    Object val = entry.getValue(); 
		    weightDataList.add(new WeightData((double)key,(int)val));
		} 
		
		
		
		
		int firstCount = 0;

		Long startTime = System.currentTimeMillis();
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		
		for (int i = 0; i < rRecords.size(); i++){
			
			FlickrData value1 = rRecords.get(i);
			
			int count = 0; 
			
			for(WeightData wd : weightDataList){
				if((wd.getKey() >= value1.getLat() - LatThreshold) && wd.getKey() <= value1.getLat() + LatThreshold){
					count++;
				}else if(wd.getKey() > value1.getLat() - LatThreshold){
					break;
				}
			}
			
			
			System.out.println(count);
			count = 0;
		    	
//		    	FlickrData value2 = sRecords.get(j);
//		    	
//		    	if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
//		    	
//		    		firstCount++;
//
// 		            long ridA = value1.getId();
// 		            long ridB = value2.getId();
// 		            if (ridA < ridB) {
// 		                long rid = ridA;
// 		                ridA = ridB;
// 		                ridB = rid;
// 		            }
//// 		            writer.write(ridA + "%" + ridB +"\n");
// 		        }
		    	
		    
		}
		writer.close();
		System.out.println(firstCount);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
