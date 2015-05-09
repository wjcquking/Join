package org.macau.flickr.knn.exact.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.knn.util.kNNUtil;

/**
 * 
 * @author hadoop
 * desc: First, T random sets of objects are selected from R,
 * 		Then , for each set, compute the total sum of the distances between every two objects.
 * 		Finally, the objects from the set with the maximum total sum distance are selected as the pivots for data partitioning.
 */
public class RandomSelelectioin {

	public static void main(String[] args){
		List<List<FlickrValue>> rList = new ArrayList<List<FlickrValue>>();
		for(int i = 0;i < kNNUtil.RANDOM_SELECTION_T;i++){
			List<FlickrValue> fList = new ArrayList<FlickrValue>();
			rList.add(fList);
			
		}
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
	            	if(random < kNNUtil.RANDOM_SELECTION_PROBABILITY){
	            		long id =Long.parseLong(value.toString().split(";")[0]);
	            		double lat = Double.parseDouble(value.toString().split(";")[1]);
	            		double lon = Double.parseDouble(value.toString().split(";")[2]);
	            		long timestamp = Long.parseLong(value.toString().split(";")[3]);
	            		
	            		
	            		outputValue.setId(id);
	            		outputValue.setLat(lat);
	            		outputValue.setLon(lon);
	            		outputValue.setTimestamp(timestamp);
	            		FlickrValue flickrValue = new FlickrValue(outputValue);
	            		for(int i = 0;i < kNNUtil.RANDOM_SELECTION_T;i++){
	            			
	            			if(rList.get(i).size() < kNNUtil.REDUCER_NUMBER){
	            				rList.get(i).add(flickrValue);
	            				count++;
	            				break;
	            			}
	            		}
	            		
	            	}
	            }
	           
	            System.out.println("The Read Count is " + readCount);
	            System.out.println("The Sample Count is " + count);
	            
	            br.close();
	            reader.close();
	           
	            double sum = 0;
	            double max = 0;
	            int maxPosition = 0;
	            //calculate total sum distance in each set
	            for(int i = 0; i < kNNUtil.RANDOM_SELECTION_T;i++){
	            	
	            	for(int j =0;j < kNNUtil.REDUCER_NUMBER;j++){
	            		
	            		for(int k = j; k < kNNUtil.REDUCER_NUMBER;k++){
//	            			System.out.println(rList.get(i).get(j));
//	            			System.out.println(rList.get(i).get(k));
	            			sum += FlickrSimilarityUtil.SpatialDistance(rList.get(i).get(j), rList.get(i).get(k));
	            		}
	            		
	            	}
	            	
	            	if(max < sum){
	            		max = sum;
	            		maxPosition = i;
	            	}
	            	sum = 0;
	            }
	            
	            System.out.println(maxPosition + " " + max);
	            
	            for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
	            	sb.append(rList.get(maxPosition).get(i).toFileString() +"\r\n");
	            }
	            
	            // write string to file
	            FileWriter writer = new FileWriter(kNNUtil.R_RANDOM_PATH);
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
