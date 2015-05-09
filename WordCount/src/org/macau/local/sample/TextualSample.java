package org.macau.local.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.util.SimilarityUtil;

public class TextualSample {
	/**
	 * the strategy group sample
	 * 
	 * The main different  with the Stream Sample is that 
	 * sample a data from each group
	 */
	public static void TextualGroupSample(FlickrData[] reservoirArray){
		
		//sample r
//		FlickrData[] reservoirArray = SpatialBlackBoxWR2.weightedBlackBoxWR2ofTextual(FlickrDataLocalUtil.rDataPath,r,SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath));
		
		Map<FlickrData,List<FlickrData>> sampleResult = new HashMap<FlickrData,List<FlickrData>>();
		
		Set<String> rSet = new HashSet<String>();
		Set<String> sSet = new HashSet<String>();
		
		for(FlickrData fd : reservoirArray){
			List<FlickrData> list = new ArrayList<FlickrData>();
			sampleResult.put(fd, list);
		}
		
		File file = new File(FlickrDataLocalUtil.sDataPath);
		
        BufferedReader reader = null;
		try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            // read the data One line one time until the null
            while ((tempString = reader.readLine()) != null) {

            	FlickrData sFD = new FlickrData(FlickrSimilarityUtil.getFlickrDataFromString(tempString.toString()));

            	System.out.println(line);
            	String textual = tempString.toString().split(":")[5];
        		
        		
        		if(!textual.equals("null")){
        			
        			for(FlickrData fd : reservoirArray){
        				
        				if(FlickrSimilarityUtil.TextualSimilarity(fd, sFD)){
        					sampleResult.get(fd).add(sFD);
        				}
        			}
        			
        		}
        		line++;
            }
            
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
		
		Iterator iter = sampleResult.entrySet().iterator();
		
		int i = 0;
		while(iter.hasNext()){
			Map.Entry<FlickrData,List<FlickrData>> entry = (Map.Entry<FlickrData,List<FlickrData>>)iter.next();
			rSet.add(entry.getKey().getTextual());
			FlickrData s = BlackBoxU2ofTextual(1,entry.getValue());
			sSet.add(s.getTextual());
			System.out.println(i+++ "   " + entry.getKey() +"   "+ entry.getValue().size() + " " +s);
			System.out.println(rSet.size());
			System.out.println(sSet.size());
		}
		
		int m = 0;
		for(String r: rSet){
			for(String s : sSet){
				
				if(r!= null && s != null){
					if(FlickrSimilarityUtil.getTokenSimilarity(r, s) > FlickrSimilarityUtil.TEXTUAL_THRESHOLD){
						System.out.println(m++);
						System.out.println(r + "    " + s);
					}
				}
			}
		}
		
		System.out.println(rSet.size());
		System.out.println(sSet.size());
		System.out.println(m);
	}
	
	/**
	 * 
	 * @param r : the sample size
	 * @param list
	 * @return One Type of Data
	 */
	public static FlickrData BlackBoxU2ofTextual(int r,List<FlickrData> list){
		
		int N = 0;
		int i = 0;
		for(FlickrData fd: list){
			N += 1;
			Random random = new Random();
			
			if(random.nextDouble() <= 1D/N){
				i = N -1 ;
			}
		}
		if(list.size() > 0){
			
			return list.get(i);
			
		}else{
			return new FlickrData();
		}
	}
	
	
	
	/**
	 * 
	 * @param rRecords
	 * @param r
	 * @param accountMap
	 * 
	 * 
	 * get the selectivity of the textual join
	 */
	public static double textualOlkenSample(ArrayList<FlickrData> rRecords, int r,Map<Integer,Integer> accountMap,int sSize){
		
		int max = OlkenSampleAlgorithm.getSumValue(accountMap);
		

		int[] iterationCount = new int[r];
		
		Map<Integer,List<FlickrData>> weightedListMap = SpatialBlackBoxWR2.getTextualListData(FlickrDataLocalUtil.sDataPath);
		
		
		for(int i = 0;i < r;i++){
			
			boolean accept = false;
			System.out.println(i);
			
			while(accept == false){
				
				iterationCount[i]++;
				
				Random random = new Random();
				
				FlickrData rFlickrData = rRecords.get(random.nextInt(rRecords.size()));
				
				String[] textualList = rFlickrData.getTextual().split(";");
				int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
				
				int weightSum = 0;
				
				if(!rFlickrData.getTextual().equals("null")){
					
					for(int j = 0; j < prefixLength;j++){
						
						
						Integer tokenID = Integer.parseInt(textualList[j]);
						
						int weight= 0;
					
						if(accountMap.get(tokenID) == null){
							weight= 0;
						}else{
							weight = accountMap.get(tokenID);
						}
						
						weightSum+=weight;
					}
					
				}else{
					weightSum = 0;
				}
				

				double p = (double)weightSum/ (double)max;
				
				Random acceptRandom = new Random();
				double pro =acceptRandom.nextDouble();
				
				if( pro <= p){
					
					Random listRandom = new Random();
					
					FlickrData sFlickrData = new FlickrData();
					
					List<FlickrData> candidateList = new ArrayList<FlickrData>();
					Map<Long,FlickrData> candidateMap = new HashMap<Long,FlickrData>();
					
					if(!rFlickrData.getTextual().equals("null")){
						for(int j = 0; j < prefixLength;j++){
							
							Integer tokenID = Integer.parseInt(textualList[j]);
							
						
							if(weightedListMap.get(tokenID) == null){
								
							}else{
								for(FlickrData fd : weightedListMap.get(tokenID)){
									candidateMap.put(fd.getId(), fd);
								}
							}
						}
					}else{
						candidateList.clear();
						candidateMap.clear();
					}
					
					Iterator iter = candidateMap.entrySet().iterator(); 
					
	//				int m = 0;
					while (iter.hasNext()) { 
	//					System.out.println(m++);
					    Map.Entry<Long,FlickrData> entry = (Map.Entry) iter.next(); 
					    Object key = entry.getKey(); 
					    FlickrData val = entry.getValue(); 
					    candidateList.add(val);
					} 
	
					
					if(candidateList.size() != 0){
						sFlickrData = candidateList.get(listRandom.nextInt(candidateList.size()));
					}
					
					
					
					if(!sFlickrData.getTextual().equals("null")&& FlickrSimilarityUtil.TextualSimilarity(rFlickrData, sFlickrData)){
							
						accept = true;
//						System.out.println(p + "  "+ iterationCount[i]+"    "+weightSum+"  "+ "   "+ rFlickrData + "%" + sFlickrData);
						
					}
				}
			}
		}
		
		double sum = 0;
		double account = 0;
		for(int i:iterationCount){
			sum+=i;
			account+=1;
		}
		
		double expectedValue = sum/account;
		
		System.out.println("The account Map size" + accountMap.size());
		System.out.println("the iteration count sum is " + sum + ":" + account );
		System.out.println("the account Map max is " + max);
		System.out.println(expectedValue);
		
		double selectivity = (double)max/(expectedValue * sSize);
		
		System.out.println("the selectivity is "+selectivity);
		
		return selectivity;
	}
	
	public static void FrequencyParitionSampleofTextual(int r, int t){
		Map<Integer,Integer> weightMap = SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Map<Integer,Integer> highWeightMap = new HashMap<Integer,Integer>();
		Map<Integer,Integer> lowWeightMap = new HashMap<Integer,Integer>();
		

		
		
		Iterator iter = weightMap.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Integer,Integer> entry = (Map.Entry<Integer,Integer>)iter.next();
			if(entry.getValue() >= t){
				
				highWeightMap.put(entry.getKey(), entry.getValue());
				
			}else{
				
				lowWeightMap.put(entry.getKey(), entry.getValue());
				
			}
		}
		
		// read R set and sample r using WR2
		double W = 0;
		
		
		// the dummy array and the final sample data
		FlickrData[] reservoirArray = new FlickrData[r]; 
		
		for(int i = 0; i < r;i++){
			reservoirArray[i] = new FlickrData();
		}
		
		File file = new File(FlickrDataLocalUtil.rDataPath);
		
        BufferedReader reader = null;
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            // read the data One line one time until the null
            while ((tempString = reader.readLine()) != null) {


            	System.out.println(line);
            	String textual = tempString.toString().split(":")[5];
        		
        		
        		if(!textual.equals("null")){
        			
        			String[] textualList = textual.split(";");
        			
        			
        			//get the prefix values
        			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
        			
        			int weight = 0;
        			
        			for(int i = 0; i < prefixLength;i++){
        				Integer tokenID = Integer.parseInt(textualList[i]);
        				
        				if(highWeightMap.containsKey(tokenID)){
        					
        				}
        				if(highWeightMap.get(tokenID) == null){
        					weight += 0;
        				}else{
        					weight += highWeightMap.get(tokenID);
        				}
        			}
        			
        			W += weight;
        			
        			for(int j = 0;j < r;j++){
        				Random random = new Random();
        				
//        				System.out.println(weight/W);
        				if(random.nextDouble() < weight/W){
        					String[] flickrData = tempString.split(":");
        	                
        	                long id = Long.parseLong(flickrData[0]);
        	                int locationID = Integer.parseInt(flickrData[1]);
        	                double lat = Double.parseDouble(flickrData[2]);
        	                double lon = Double.parseDouble(flickrData[3]);
        	                
        	        		long timestamp = Long.parseLong(flickrData[4]);
        	        		
        	        		int[] tokens = new int[textual.split(";").length];
        	        		
        	        		for(int i = 0; i < textual.split(";").length;i++){
        	        			tokens[i] = Integer.parseInt(textual.split(";")[i]);
        	        		}
        					
        					reservoirArray[j] = new FlickrData(id, locationID,lat, lon, timestamp, textual,tokens);
        				}
        			}
        		}
        		
        		
                
                line++;
            }
           
            
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        
        int i = 0;
        for(FlickrData fd: reservoirArray){
        	System.out.println(i++ + "  "+fd.toString());
        	
        }
		
	}
	public static void main(String[] args){
		int r = 1000;
		int sSize = 10000;
		Long startTime = System.currentTimeMillis();
		//get the account information from S
//		Map<Integer,Integer> accountMap = SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath);
		
		//get r size of sample data
//		FlickrData[] reservoirArray = SpatialBlackBoxWR2.weightedBlackBoxWR2ofTextual(FlickrDataLocalUtil.rDataPath,r,accountMap);
		
//		TextualGroupSample(reservoirArray);
		
		textualOlkenSample(ReadFlickrData.readFileBySampling(FlickrDataLocalUtil.rDataPath,1),r,SpatialBlackBoxWR2.getTextualWeightedData(FlickrDataLocalUtil.sDataPath),sSize);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
