package org.macau.flickr.knn.exact.preprocessing;

/**
 * @author hadoop
 * @date 2013-12-1
 * desc:
 * First, Sample from the original data set R, and randomly select an object as the first pivot
 * Second, the object with the largest distance to the first pivot is selected as the second pivot
 * Third, in the ith iteration, the object that maximizes the sum of its distance to the first i-1 object 
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class FarthestSelection {

	public static void main(String[] args){
		//store the sample list
		List<FlickrValue> rList = new ArrayList<FlickrValue>();
		
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
	            	if(random < kNNUtil.FARTHEST_SELECTION_PROBABILITY){
	            		long id =Long.parseLong(value.toString().split(";")[0]);
	            		double lat = Double.parseDouble(value.toString().split(";")[1]);
	            		double lon = Double.parseDouble(value.toString().split(";")[2]);
	            		long timestamp = Long.parseLong(value.toString().split(";")[3]);
	            		
	            		
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
	            int maxPosition = 0;
	            //calculate total sum distance in each set
	            int random = (int) (Math.random() * rList.size())%rList.size();
	            
	            List<FlickrValue> pivotList = new ArrayList<FlickrValue>();
	            pivotList.add(new FlickrValue(rList.get(random)));
	            rList.remove(random);
	            
	            for(int k = 0; k < kNNUtil.REDUCER_NUMBER;k++){
		            for(int i = 0; i < rList.size();i++){
		            	for(int j = 0;j < pivotList.size();j++){
		            		sum += FlickrSimilarityUtil.SpatialDistance(rList.get(i), pivotList.get(j));
		            	}
		            	if(max < sum){
		            		max = sum;
		            		maxPosition = i;
		            	}
		            	sum = 0;
		            	
		            }
		            
		           // System.out.println(maxPosition + " " + max);
		            sb.append(rList.get(maxPosition).toFileString() +"\r\n");
		            pivotList.add(new FlickrValue(rList.get(maxPosition)));
	            	rList.remove(maxPosition);
	            }

	            
	            
	            
	            // write string to file
	            FileWriter writer = new FileWriter(kNNUtil.R_FARTHEST_PATH);
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
