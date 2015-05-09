package org.macau.local.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.local.util.FlickrData;

public class OlkenSampleAlgorithm {

	public static void createData(int randomSize,String outputPath){
		
		//String outputPath ="D:\\Data\\Relation Data\\r.data";
		try
		{

			FileWriter writer = new FileWriter(outputPath);
			Random rRandom = new Random();
			for(int i = 0;i < 10000;i++){
				writer.write(rRandom.nextInt(randomSize) + "\n");
			}
			
			writer.close();
		}
		
		catch(IOException iox)
		{
			System.out.println("Problemwriting" + outputPath);
		}
	}
	
	public static void UsingRandomData(){
		// use two list to store the data
			List<Integer> rList = new ArrayList<Integer>();
			List<Integer> sList = new ArrayList<Integer>();
			
			Map<Integer,Integer> rAccountMap = new HashMap<Integer,Integer>();
			Map<Integer,Integer> accountMap = new HashMap<Integer,Integer>();
			
			Random rRandom = new Random();
			
			int dataSize = 10000;
			int randomSize = 1000;
			
			for(int i = 0; i < dataSize;i++){
				
				int temp = rRandom.nextInt(randomSize);
				
				rList.add(temp);
				
				if(rAccountMap.get(temp) == null){
	              	
					rAccountMap.put(temp, 1);
	            	
				}else{
	            	
	        	   rAccountMap.put(temp, rAccountMap.get(temp)+1);
	            	
				}
			}
			
			
		
			
			for(int i = 0; i < dataSize;i++){
				
				int temp = rRandom.nextInt(randomSize);
				sList.add(temp);
				
				if(accountMap.get(temp) == null){
	              	
					 accountMap.put(temp, 1);
	             	
	            }else{
	             	
	            	 accountMap.put(temp, accountMap.get(temp)+1);
	             	
	            }
			}
			int max = 0;
			Iterator iter = accountMap.entrySet().iterator();
			
			int count = 0;
			
			while(iter.hasNext()){
				Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>)iter.next();
				count+=entry.getValue();
				if(entry.getValue() > max){
					max = entry.getValue();
				}
			}
			max *= 10;
			int rsSum = 0;
			
			for(int i = 0; i < randomSize;i++){
				int r = 0; 
				int s = 0;
				if(rAccountMap.get(i) != null){
					r = rAccountMap.get(i);
				}
				if(accountMap.get(i) != null){
					s = accountMap.get(i);
				}
				
				rsSum += r*s;
			}
			
			int r = 2000;
			
			int[] iterationCount = new int[r];
			
			
			int candinateCount = 0;
			
			for(int i = 0;i < r;i++){
				
				boolean accept = false;
				
				while(accept == false){
					
					iterationCount[i]++;
					
					Random random = new Random();
					
					int rValue = rList.get(random.nextInt(rList.size()));
					
					int weight = 0;
					
					if(accountMap.get(rValue) == null){
						weight= 0;
					}else{
						weight = accountMap.get(rValue);
					}
					
					double p = (double)weight/ (double)max;
					
					Random acceptRandom = new Random();
					double pro =acceptRandom.nextDouble();
					
					if( pro <= p){
						
						candinateCount+=weight;
						
						accept = true;
						System.out.println(p + "  "+ iterationCount[i]+"    "+weight+"  ");
						
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
			System.out.println("candinate count is " + candinateCount);
			
			System.out.println(expectedValue);
			double selectivity = (double)max/(expectedValue * sList.size());
			
			System.out.println(selectivity);
			System.out.println(rsSum);
			System.out.println((double)rsSum/(rList.size()*sList.size()));
				
	}
	
	public static List<Integer> readData(String path){
		List<Integer> list = new ArrayList<Integer>();
		File file = new File(path);
        BufferedReader reader = null;
        
        try {
            reader = new BufferedReader(new FileReader(file));
            
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {

                int temp = Integer.parseInt(tempString);
                list.add(temp);
                
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
		return list;
	}
	
	public static Map<Integer,Integer> getAccountMap(List<Integer> list){
		Map<Integer,Integer> accountMap = new HashMap<Integer,Integer>();
		
		for(int temp :list){
			
			if(accountMap.get(temp) == null){
              	
				 accountMap.put(temp, 1);
            	
           }else{
            	
           	 accountMap.put(temp, accountMap.get(temp)+1);
            	
           }
		}
		
		return accountMap;
	}
	
	/**
	 * 
	 * @param accountMap
	 * @return the Max value in the Map
	 */
	public static int getMaxValue(Map<Integer,Integer> accountMap){
		int max = 0;
		Iterator iter = accountMap.entrySet().iterator();
		
		while(iter.hasNext()){
			Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>)iter.next();
			if(entry.getValue() > max){
				max = entry.getValue();
			}
		}
		return max;
	}
	
	/**
	 * 
	 * @param accountMap
	 * @return the sum of all the map value
	 */
	public static int getSumValue(Map<Integer,Integer> accountMap){
		int sum = 0;
		Iterator iter = accountMap.entrySet().iterator();
		
		while(iter.hasNext()){
			Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>)iter.next();
			sum+=entry.getValue();
		}
		
		return sum;
	}
	/**
	 * 
	 * @param args
	 * just test the Olken algorithm
	 * 
	 * the way to create the data:
	 * 
	 * the R: 10000,from 1 to 1000
	 * the S: 10000,from 1 to 1000
	 */
	public static void main(String[] args){
		int randomSize = 1000;
		System.out.println("In the Relation Database situation");
		
		String rPath = "D:\\Data\\Relation Data\\r.data";
		String sPath = "D:\\Data\\Relation Data\\s.data";
		
//		createData(randomSize,rPath);
//		createData(randomSize,sPath);
		
		List<Integer> rList = readData(rPath);
		List<Integer> sList = readData(sPath);
		
		Map<Integer,Integer> rAccountMap = getAccountMap(rList);
		Map<Integer,Integer> accountMap = getAccountMap(sList);
		
		int max = getMaxValue(accountMap);
	
		int rsSum = 0;
		
		for(int i = 0; i < randomSize;i++){
			int r = 0; 
			int s = 0;
			if(rAccountMap.get(i) != null){
				r = rAccountMap.get(i);
			}
			if(accountMap.get(i) != null){
				s = accountMap.get(i);
			}
			
			rsSum += r*s;
		}
		
		int r = 1000;
		
		int[] iterationCount = new int[r];
		
		
		int candinateCount = 0;
		
		for(int i = 0;i < r;i++){
			
			boolean accept = false;
			
			while(accept == false){
				
				iterationCount[i]++;
				
				Random locationRandom = new Random();
				
				int rValue = rList.get(locationRandom.nextInt(rList.size()));
				
				int weight = 0;
				
				if(accountMap.get(rValue) == null){
					weight= 0;
				}else{
					weight = accountMap.get(rValue);
				}
				
				double p = (double)weight/ (double)max;
				
				Random acceptRandom = new Random();
				double pro =acceptRandom.nextDouble();
				
				if( pro <= p){
					
					candinateCount+=weight;
					
					accept = true;
					System.out.println(p + "  "+ iterationCount[i]+"    "+weight+"  ");
					
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
		System.out.println("candinate count is " + candinateCount);
		
		System.out.println(expectedValue);
		double selectivity = (double)max/(expectedValue * sList.size());
		
		System.out.println(selectivity*10);
		System.out.println(rsSum);
		System.out.println((double)rsSum/(rList.size()*sList.size()));
		
	}
}
