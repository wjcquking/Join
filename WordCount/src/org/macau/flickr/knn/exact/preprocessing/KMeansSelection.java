package org.macau.flickr.knn.exact.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.macau.flickr.knn.exact.preprocessing.kmeans.kmeans;
import org.macau.flickr.knn.exact.preprocessing.kmeans.kmeans_data;
import org.macau.flickr.knn.exact.preprocessing.kmeans.kmeans_param;
import org.macau.flickr.knn.util.kNNUtil;
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
		List<double[]> pointList = new ArrayList<double[]>();
		try {
	            StringBuffer sb= new StringBuffer("");
	           
	            FileReader reader = new FileReader(kNNUtil.R_FILE_PATH);
	            BufferedReader br = new BufferedReader(reader);
	            
	            String value = null;
	           
	            int count = 0;
	            int readCount = 0;
	            while((value = br.readLine()) != null) {
	            	readCount++;
	            	double random = Math.random();
	            	if(random < kNNUtil.KMEANS_SELECTION_PROBABILITY){
	            		
	            		double lat = Double.parseDouble(value.toString().split(";")[1]);
	            		double lon = Double.parseDouble(value.toString().split(";")[2]);
	            		
	            		pointList.add(new double[]{lat,lon});


	            		count++;
	            	}
	            }
	           
	            System.out.println("The Read Count is " + readCount);
	            System.out.println("The Sample Count is " + count);
	            
	            br.close();
	            reader.close();
	           
	            double[][] points = pointList.toArray(new double[0][0]);
	    		
	            //initial the data structure
	    		kmeans_data data = new kmeans_data(points,pointList.size(), 2);
	    		kmeans_param param = new kmeans_param(); //初始化参数结构
	    		param.initCenterMehtod = kmeans_param.CENTER_RANDOM; //设置聚类中心点的初始化模式为随机模式
	    	   
	    		//the first parameter is the number of cluster
	    		kmeans.doKmeans(kNNUtil.REDUCER_NUMBER, data, param);
	    	  
	    		//查看每个点的所属聚类标号
//	    		System.out.print("The labels of points is: ");
//	    		for (int lable : data.labels) {
//	    			System.out.print(lable + "  ");
//	    		}
//	    		
//	    		System.out.println(data.centers);
	    		
	    		count = 1;
	    		for(double[] i: data.centers){
	    			sb.append(count++ + ";");
	    			for(double j: i){
	    				sb.append(j+";");
//	    				System.out.print(j + "  ");
	    			}
	    			sb.append(1 +"\r\n");
//	    			System.out.println();
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
