package org.macau.local.ego;

/**
 * 
 * This file is for data preparation of the EGO join
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.util.FlickrSimilarityUtil;
//import org.macau.local.util.FlickrDataLocalUtil;

public class ReadOutputData {

	
	
	public static void writeToFile(int i){
		
		File file = new File("D:\\360Downloads\\output_14\\part-s-00000");
        BufferedReader reader = null;
        
        int rCount = 0;
        int sCount = 0;
        
        int GROUPNUMBER = 250;
        int TotalNumber = 400000;
        System.out.println("good");
        
        try {
            System.out.println("Read one line");
            
            int[] groupArray = {0,10,20,30,40};
            
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_14_g/part-s-0000" + i;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int tag = Integer.parseInt(values[1]);
            	
            
            	if(tag == FlickrSimilarityUtil.S_tag){
            		sCount++;
            		int group = sCount/GROUPNUMBER;
            		tempString = group + " " + tempString;
            		if(group < groupArray[i+1] && group >= groupArray[i]){
            			writer.write(tempString + "\n");
            		}
            		
            		
            	}else if (tag == FlickrSimilarityUtil.R_tag){
            		rCount++;
            		int group = rCount/GROUPNUMBER;
            		tempString = group + " " + tempString;
            		
            	}
            	
            }
            
            System.out.println(sCount);
            System.out.println(rCount);
            reader.close();
            writer.close();
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
	}
	
	
	public static void getStatistic(){
		File file = new File("D:\\360Downloads\\output3\\part-r-00000");
        BufferedReader reader = null;
        
        int rCount = 0;
        int sCount = 0;
        
        int GROUPNUMBER = 20000;
        System.out.println("good");
        
        try {
            System.out.println("Read one line");
            
            int[] groupArray = {0,5,10,15,20};
            
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/statistic/part-r-00003";
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
            
            Map<Integer,Integer> statisticMap = new  HashMap<Integer,Integer>();
            
            
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int tag = Integer.parseInt(values[0]);
            	int integer = Integer.parseInt(values[1]);
            	
            
            	if(tag == FlickrSimilarityUtil.S_tag){
            		sCount++;
            		if(statisticMap.containsKey(integer)){
            			statisticMap.put(integer, statisticMap.get(integer)+1);
            		}else{
            			statisticMap.put(integer, 1);
            		}
            		
            	}else if (tag == FlickrSimilarityUtil.R_tag){
            		rCount++;
            		int group = rCount/GROUPNUMBER;
            		tempString = group + " " + tempString;
            		
            	}
            	
            }
            
            int sum = 0;
            for(java.util.Iterator<Integer> i = statisticMap.keySet().iterator();i.hasNext();){
            	int obj = i.next();
            	sum += statisticMap.get(obj);
            	System.out.println(obj + "  " + statisticMap.get(obj));
            }
            
            System.out.println(statisticMap.size());
            System.out.println(sum);
            System.out.println(sCount);
            System.out.println(rCount);
            reader.close();
            writer.close();
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
	}
	public static void main(String[] args){
		for(int i = 0; i < 4;i++){
			writeToFile(i);
		}
//		getStatistic();
		System.out.println("gg");
	}
		
		
}
