package org.macau.local.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.spatial.Distance;
import org.macau.util.SimilarityUtil;

/**
 * 
 * @author mb25428
 * Using the method of Black box WR2 in the paper of "On Random Sampling over join"
 * R: the base data
 * S: the weight data which we can get the statistic information
 */
public class SpatialBlackBoxWR2 {

	public static final int sampleSize  = 1000;
	
	/**
	 * 
	 * @param fileName
	 * @return get the histogram of R data set
	 */
	public static Map<Integer,Integer> getRTemporalWeightedData(String fileName){
		//read each record from the file
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,Integer> temporalWeight = new HashMap<Integer,Integer>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //read all the data from S and calculate the result
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
                long timestamp = Long.parseLong(flickrData[4]);
        		
        		
        		int timeInterval = (int)(timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD);
              int i = timeInterval;
//                for(int i = timeInterval-1;i <= timeInterval+1;i++){
                		
                		 if(temporalWeight.get(i) == null){
                         	
                			 temporalWeight.put(i, 1);
                         	
                         }else{
                         	
                        	 temporalWeight.put(i, temporalWeight.get(i)+1);
                         }
                         	
//                }
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
		
		return temporalWeight;
	}
	
	/**
	 * 
	 * @param fileName
	 * @return get the candidate histogram of S data set
	 * which means for each part, it need save the candidate part
	 */
	public static Map<Integer,Integer> getTemporalWeightedData(String fileName){
		//read each record from the file
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,Integer> temporalWeight = new HashMap<Integer,Integer>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //read all the data from S and calculate the result
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
                long timestamp = Long.parseLong(flickrData[4]);
        		
        		
        		int timeInterval = (int)(timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD);
//              int i = timeInterval;
                for(int i = timeInterval-1;i <= timeInterval+1;i++){
                		
                		 if(temporalWeight.get(i) == null){
                         	
                			 temporalWeight.put(i, 1);
                         	
                         }else{
                         	
                        	 temporalWeight.put(i, temporalWeight.get(i)+1);
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
		
		return temporalWeight;
	}
	
	
	public static Map<Integer,List<FlickrData>> getTemporalListData(String fileName){
		
		//read each record from the file
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,List<FlickrData>> temporalWeight = new HashMap<Integer,List<FlickrData>>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            //read all the data from S and calculate the result
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
                long timestamp = Long.parseLong(flickrData[4]);
        		
        		
        		int timeInterval = (int)(timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD);
                
                
                
                FlickrData fd = FlickrSimilarityUtil.getFlickrDataFromString(tempString);
                
                
                for(int i = timeInterval-1;i <= timeInterval+1;i++){
                	
                		if(temporalWeight.containsKey(i)){
        					
                			temporalWeight.get(i).add(fd);

        				}else{

        					ArrayList<FlickrData> recordList = new ArrayList<FlickrData>();
        					recordList.add(fd);
        					temporalWeight.put(new Integer(i),recordList);
        					
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
		
		return temporalWeight;
	}

	/**
	 * Get the spatial statistic information from the set S
	 * 
	 * divide whole space into many tiles, for each tile, the nine tiles surrounded by this tile
	 * 
	 * 1 2 3
	 * 4 5 6
	 * 7 8 9
	 * 
	 * for 5, the weight is the sum of the nine tiles's number
	 * 
	 * Now the distance is calculated by the Euclidean distance
	 */
	public static Map<Integer,Integer> getSpatialWeightedData(String fileName){
		
		//read each record from the file
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,Integer> spatialWeight = new HashMap<Integer,Integer>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //read all the data from S and calculate the result
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
                double lat = Double.parseDouble(flickrData[2]);
                double lon = Double.parseDouble(flickrData[3]);
                
                
                
                int latTile =(int)(lat/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                int lonTile = (int)(lon/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                
                for(int i = latTile-1;i <= latTile+1;i++){
                	
                	for(int j = lonTile-1; j <= lonTile+1;j++){
                		
                		int zOrder = ZOrderValue.parseToZOrder(i, j);
                		
                		 if(spatialWeight.get(zOrder) == null){
                         	
                			 spatialWeight.put(zOrder, 1);
                         	
                         }else{
                         	
                        	 spatialWeight.put(zOrder, spatialWeight.get(zOrder)+1);
                         	
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
		
		return spatialWeight;
	}
	
	
	public static Map<Integer,List<FlickrData>> getSpatialListData(String fileName){
		
		//read each record from the file
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,List<FlickrData>> spatialWeight = new HashMap<Integer,List<FlickrData>>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            //read all the data from S and calculate the result
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
                double lat = Double.parseDouble(flickrData[2]);
                double lon = Double.parseDouble(flickrData[3]);
                
                FlickrData fd = FlickrSimilarityUtil.getFlickrDataFromString(tempString);
                
                int latTile =(int)(lat/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                int lonTile = (int)(lon/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                
                for(int i = latTile-1;i <= latTile+1;i++){
                	
                	for(int j = lonTile-1; j <= lonTile+1;j++){
                		
                		int zOrder = ZOrderValue.parseToZOrder(i, j);
                		
                		
                		if(spatialWeight.containsKey(zOrder)){
        					
                			spatialWeight.get(zOrder).add(fd);

        				}else{

        					ArrayList<FlickrData> recordList = new ArrayList<FlickrData>();
        					recordList.add(fd);
        					spatialWeight.put(new Integer(zOrder),recordList);
        					
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
		
		return spatialWeight;
	}
	
	/**
	 * 
	 * @param fileName
	 * @return statistic of the data
	 * Using the Account as the weight
	 */
	public static Map<Integer,Integer> getTextualWeightedData(String fileName){
		
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,Integer> textualAccount = new HashMap<Integer,Integer>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String value = null;
            int line = 1;
            
            // Read One line one time until the null
            while ((value = reader.readLine()) != null) {
        		
        		String textual = value.toString().split(":")[5];
        		
        		
        		if(!textual.equals("null")){
        			
        			String[] textualList = textual.split(";");
        			
        			
        			//get the prefix values
        			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
        			
        			for(int i = 0; i < prefixLength;i++){
        				
        				Integer tokenID = Integer.parseInt(textualList[i]);
        				
        				if(textualAccount.get(tokenID) == null){
        					
        					textualAccount.put(tokenID, 1);
        					
        				}else{
        					
        					textualAccount.put(tokenID, textualAccount.get(tokenID) + 1);
        					
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
        return textualAccount;
	}

	
	/**
	 * 
	 * @param fileName
	 * @return statistic of the data
	 * Using the Account as the weight
	 */
	public static Map<Integer,List<FlickrData>> getTextualListData(String fileName){
		
		File file = new File(fileName);
		 
        BufferedReader reader = null;
        
        Map<Integer,List<FlickrData>> textualWeight = new HashMap<Integer,List<FlickrData>>();
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            // Read One line one time until the null
            while ((tempString = reader.readLine()) != null) {
        		
        		String textual = tempString.toString().split(":")[5];
        		
        		System.out.println(line);
        		if(!textual.equals("null")){
        			
        			String[] textualList = textual.split(";");
        			
        			FlickrData fd = FlickrSimilarityUtil.getFlickrDataFromString(tempString);
        			//get the prefix values
        			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
        			
        			for(int i = 0; i < prefixLength;i++){
        				
        				Integer tokenID = Integer.parseInt(textualList[i]);
        				
        				if(textualWeight.containsKey(tokenID)){
        					
        					textualWeight.get(tokenID).add(fd);

        				}else{

        					List<FlickrData> recordList = new ArrayList<FlickrData>();
        					recordList.add(fd);
        					textualWeight.put(new Integer(tokenID),recordList);
        					
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
        return textualWeight;
	}
	/**
	 * get the weight data from the S set
	 */
	public static void getWeightedData(){
		
	}
	
	
	public static void getTokenWeight(){
		
	}
	
	
	public static int[] unWeightedBlackBoxWR2(int n, int r, List<Integer> tuples){
		int N = 0;
		
		int[] reservoirArray = new int[r]; 
		for(int i = 0; i < r;i++){
			reservoirArray[i] = 0;
		}
		
		for(Integer tuple : tuples){
			N += 1;
			for(int j = 0;j < r;j++){
				Random random = new Random();
				System.out.println("12" + 1D/N);
				if(random.nextDouble() < 1D/N){
					reservoirArray[j] = tuple;
				}
			}
		}
		
		return reservoirArray;
	}
	
	
	
	
	
	/*
	 * the size of R set is no needed
	 * r is the sample size of the data
	 */
	public static FlickrData[] weightedBlackBoxWR2ofTextual(String filePath,int r,Map<Integer,Integer> accountMap){
		
		double W = 0;
		
		
		// the dummy array and the final sample data
		FlickrData[] reservoirArray = new FlickrData[r]; 
		
		for(int i = 0; i < r;i++){
			reservoirArray[i] = new FlickrData();
		}
		
		File file = new File(filePath);
		
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
        				
        				if(accountMap.get(tokenID) == null){
        					weight += 0;
        				}else{
        					weight += accountMap.get(tokenID);
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
		
        return reservoirArray;
	}
	
	
	/*
	 * the size of R set is no needed
	 * r is the sample size of the data
	 */
	public static FlickrData[] weightedBlackBoxWR2ofSpatial(String filePath,int r,Map<Integer,Integer> accountMap){
		
		double W = 0;
		
		
		// the dummy array and the final sample data
		FlickrData[] reservoirArray = new FlickrData[r]; 
		
		for(int i = 0; i < r;i++){
			reservoirArray[i] = new FlickrData();
		}
		
		File file = new File(filePath);
		
        BufferedReader reader = null;
        
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            // read the data One line one time until the null
            while ((tempString = reader.readLine()) != null) {


            	System.out.println(line);
        		
        			
                double lat = Double.parseDouble(tempString.split(":")[2]);
                double lon = Double.parseDouble(tempString.split(":")[3]);
                
                int latTile =(int)(lat/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                int lonTile = (int)(lon/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
    			
    			
                int weight = 0;
                
    			int zOrder = ZOrderValue.parseToZOrder(latTile, lonTile);
    				
				if(accountMap.get(zOrder) == null){
					weight = 0;
				}else{
					weight += accountMap.get(zOrder);
    			}
    			
    			W += weight;
    			
    			for(int j = 0;j < r;j++){
    				Random random = new Random();
    				
    				if(random.nextDouble() < weight/W){
    					String[] flickrData = tempString.split(":");
    					String textual = tempString.toString().split(":")[5];
    	                long id = Long.parseLong(flickrData[0]);
    	                int locationID = Integer.parseInt(flickrData[1]);
    	                
    	        		long timestamp = Long.parseLong(flickrData[4]);
    	        		
    	        		if(!textual.equals("null")){
	    	        		int[] tokens = new int[textual.split(";").length];
	    	        		for(int i = 0; i < textual.split(";").length;i++){
	    	        			
	    	        			tokens[i] = Integer.parseInt(textual.split(";")[i]);
	    	        		}
	    	        		
	    	        		reservoirArray[j] = new FlickrData(id, locationID,lat, lon, timestamp, textual,tokens);
	    	        		
    	        		}else{
    	        			
    	        			reservoirArray[j] = new FlickrData(id, locationID,lat, lon, timestamp, textual,new int[0]);
    	        			
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
		
        return reservoirArray;
	}
	public static int[] weightedBlackBoxWR2(int n, int r, List<Integer> tuples, Map<Integer,Double> weightMap){
		
		double W = 0;
		
		int[] reservoirArray = new int[r]; 
		
		for(int i = 0; i < r;i++){
			reservoirArray[i] = 0;
		}
		
		
		for(Integer tuple : tuples){
			W += weightMap.get(tuple);
			for(int j = 0;j < r;j++){
				Random random = new Random();
				System.out.println(weightMap.get(tuple)/W);
				if(random.nextDouble() < weightMap.get(tuple)/W){
					reservoirArray[j] = tuple;
				}
			}
		}
		
		return reservoirArray;
	}
	
	public static void BlackBoxU2(){
		int N = 0;
		int[] reservoirArray = new int[sampleSize]; 
		for(int i = 0; i < sampleSize;i++){
			reservoirArray[i] = 0;
		}
	}
	
	
	public static void BlackBoxU2ofTextual(){
		
	}
	
	public static void BlackBoxU2ofSpatial(){
		
	}
	public static void BlackBoxU1(){
		
	}
	public static void main(String[] args){
		
//		weightedBlackBoxWR2ofTextual(FlickrDataLocalUtil.rDataPath,4000,getTextualWeightedData(FlickrDataLocalUtil.sDataPath));
//		weightedBlackBoxWR2ofSpatial(FlickrDataLocalUtil.rDataPath,4000,getSpatialWeightedData(FlickrDataLocalUtil.sDataPath));
		SpatialBlackBoxWR2.getSpatialListData(FlickrDataLocalUtil.sDataPath);
		
		//r dummy values
//		int[] reservoirArray = new int[sampleSize]; 
//		for(int i = 0; i < sampleSize;i++){
//			reservoirArray[i] = 0;
//		}
//		int n = 100;
//		List<Integer> tuples = new ArrayList<Integer>();
//		for(int i = 0; i < n;i++){
//			tuples.add(i);
//		}
//		
//		Map<Integer,Double> weightMap = new HashMap<Integer,Double>();
//		for(int i =0; i < n;i++){
//			weightMap.put(i, (i+1)/100D);
//		}
//		
//		int[] reservoirArray = weightedBlackBoxWR2(n,10,tuples,weightMap);
//		for(int i : reservoirArray){
//			System.out.println(i);
//		}
		
		
		
		
		
		
		
//		Iterator iter = weightMap.entrySet().iterator();
//		while(iter.hasNext()){
//			Map.Entry<Integer, Double> entry = (Map.Entry<Integer, Double>)iter.next();
//			
//		}
		
		//get every tuple's weight from the other set
//		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		
		//get every tuple from the Relation R
		
		//using the probability to get right result
		
		
		//when read all the result, get the final sample
		
		
	}
}
