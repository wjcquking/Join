package org.macau.flickr.knn.exact.preprocessing.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author hadoop
 * date: 2013-12-1
 * desc:
 */



public class KMeansSelection {
	public static List<FlickrValue> meanValue(Map<FlickrValue,List<FlickrValue>> pivotMap){
		List<FlickrValue> meanList = new ArrayList<FlickrValue>();
		
		for(Entry<FlickrValue, List<FlickrValue>> fv: pivotMap.entrySet()){
			
			double sumLat = 0;
        	double sumLon = 0;
        	int size = fv.getValue().size();
        	for(int j = 0; j < size;j++){
        		sumLat += fv.getValue().get(j).getLat();
        		sumLon += fv.getValue().get(j).getLon();
        	}
        	
        	double meanLat = sumLat/size;
        	double meanLon = sumLon/size;
        	
        	FlickrValue fValue = new FlickrValue();
        	fValue.setLat(meanLat);
        	fValue.setLon(meanLon);
        	
        	meanList.add(fValue);
		}
		
		return meanList;
		
	}
	
	public static void main(String[] args){
		//store the sample list
		List<FlickrValue> rList = new ArrayList<FlickrValue>();
		List<double[]> pointList = new ArrayList<double[]>();
		try {
	            StringBuffer sb= new StringBuffer("");
	           
	            FileReader reader = new FileReader(kNNUtil.R_FILE_PATH);
	            BufferedReader br = new BufferedReader(reader);
	            
	            FlickrValue outputValue = new FlickrValue();
	            String value = null;
	           
	            int count = 0;
	            int readCount = 0;
	            while((value = br.readLine()) != null) {
	            	readCount++;
	            	double random = Math.random();
	            	if(random < kNNUtil.KMEANS_SELECTION_PROBABILITY){
	            		long id =Long.parseLong(value.toString().split(";")[0]);
	            		double lat = Double.parseDouble(value.toString().split(";")[1]);
	            		double lon = Double.parseDouble(value.toString().split(";")[2]);
	            		long timestamp = Long.parseLong(value.toString().split(";")[3]);
	            		
	            		pointList.add(new double[]{lat,lon});
	            		outputValue.setId(id);
	            		outputValue.setLat(lat);
	            		outputValue.setLon(lon);
	            		outputValue.setTimestamp(timestamp);
	            		FlickrValue flickrValue = new FlickrValue(outputValue);
	            		
	            		rList.add(flickrValue);
	            		count++;
	            	}
	            }
	           
	            System.out.println("The Read Count is " + readCount);
	            System.out.println("The Sample Count is " + count);
	            
	            br.close();
	            reader.close();
	           
	            double sum = 0;
	            double max = 0;
	            double distance = 0;
	            double min = 10000;
	            int maxPosition = 0;
	            int minPosition = 0;
	            //calculate total sum distance in each set
	            int random = (int) (Math.random() * rList.size())%rList.size();
	            
	            List<FlickrValue> pivotList = new ArrayList<FlickrValue>();
	            Map<FlickrValue,List<FlickrValue>> pivotMap = new HashMap<FlickrValue,List<FlickrValue>>();
	            Map<FlickrValue,List<FlickrValue>> newPivotMap = new HashMap<FlickrValue,List<FlickrValue>>();
	            
	            Random rand = new Random();
	            boolean[] bool = new boolean[rList.size()];
	            
	            int num = 0;
	            for(int i = 0;i < rList.size();i++){
	            	do{
	            		num = rand.nextInt(rList.size());
	            		
	            	}while(bool[num]);
	            	
	            	bool[num] = true;
	            	if(pivotList.size() < kNNUtil.REDUCER_NUMBER){
	            		pivotList.add(new FlickrValue(rList.get(i)));
	            		List<FlickrValue> list  = new ArrayList<FlickrValue>();
	            		pivotMap.put(new FlickrValue(rList.get(i)), list);
	            	}
	            }
	       
	            
	            
	            
	            //put the value to the minimize distance pivot set
	            for(int i = 0; i < rList.size();i++){
	            	for(int j = 0; j < pivotList.size();j++){
	            		distance = FlickrSimilarityUtil.SpatialDistance(rList.get(i), pivotList.get(j));
	            		if(min > distance){
		            		min = distance;
		            		minPosition = j;
		            	}
		            	min = 100000;
		            	
	            	}
	            	pivotMap.get(pivotList.get(i)).add(new FlickrValue(rList.get(minPosition)));
	            }
	            
	            
	            List<FlickrValue> meanList = new ArrayList<FlickrValue>();
	            for(int i =0; i < pivotList.size();i++){
	            	double sumLat = 0;
	            	double sumLon = 0;
	            	int size = pivotMap.get(pivotList.get(i)).size();
	            	for(int j = 0; j < size;j++){
	            		sumLat += pivotMap.get(pivotList.get(i)).get(j).getLat();
	            		sumLon += pivotMap.get(pivotList.get(i)).get(j).getLon();
	            	}
	            	
	            	double meanLat = sumLat/size;
	            	double meanLon = sumLon/size;
	            	
	            	FlickrValue fv = new FlickrValue();
	            	fv.setLat(meanLat);
	            	fv.setLon(meanLon);
	            	pivotMap.put(fv, pivotMap.get(pivotList.get(i)));
	            	
	            	meanList.add(fv);
	            	pivotMap.remove(pivotList.get(i));
	            }
	            
//	            for()
	            
	            for(int k = 0; k < kNNUtil.REDUCER_NUMBER;k++){
		            for(int i = 0; i < rList.size();i++){
		            	for(int j = 0;j < pivotList.size();j++){
		            		sum += FlickrSimilarityUtil.SpatialDistance(rList.get(i), pivotList.get(j));
		            	}
		            
		            	
		            }
		            
		            System.out.println(maxPosition + " " + max);
		            sb.append(rList.get(maxPosition).toFileString() +"\r\n");
		            pivotList.add(new FlickrValue(rList.get(maxPosition)));
	            	rList.remove(maxPosition);
	            }

	            
	            
	            
	            // write string to file
	            FileWriter writer = new FileWriter(kNNUtil.R_KMEANS_PATH);
	            BufferedWriter bw = new BufferedWriter(writer);
	            bw.write(sb.toString());
	           
	            bw.close();
	            System.out.println("Over");
	            writer.close();
	      }
	      catch(FileNotFoundException e) {
	                  e.printStackTrace();
	            }
	            catch(IOException e) {
	                  e.printStackTrace();
	            }
		}
}
