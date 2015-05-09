package org.macau.local.analysis.time;

/**
 * The file is used for test the time for different times for the flickr data
 * 
 * 
 */

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;


public class ThreeFeatureJoin {
	
	  // Store the Store
	  private Stack<Object> stack = new Stack<Object>();

	  /**
	   * 获得指定数组从指定开始的指定数量的数据组合<br>
	   * 
	   * @param arr 指定的数组
	   * @param begin 开始位置
	   * @param num 获得的数量
	   */
	  public void getSequence(Object[] arr, int begin, int num) {
	    if (num == 0) {
	      System.out.println(stack); // 找到一个结果
	    } else {
	      // 循环每个可用的元素
	      for (int i = begin; i < arr.length; i++) {
	        // 当前位置数据放入结果堆栈
	        stack.push(arr[i]);
	        // 将当前数据与起始位置数据交换
	        swap(arr, begin, i);
	        // 从下一个位置查找其余的组合
	        getSequence(arr, begin + 1, num - 1);
	        // 交换回来
	        swap(arr, begin, i);
	        // 去除当前数据
	        stack.pop();
	      }
	    }
	  }
	  
	  

	  /**
	   * 交换2个数组的元素
	   * 
	   * @param arr 数组
	   * @param from 位置1
	   * @param to 位置2
	   */
	  public static void swap(Object[] arr, int from, int to) {
	    if (from == to) {
	      return;
	    }
	    Object tmp = arr[from];
	    arr[from] = arr[to];
	    arr[to] = tmp;
	  }

	public static void main(String[] args) throws IOException{
		
		List<String> orderList = new ArrayList<String>();
		
		int featureSize = 3;
		/*
		 * 1: Temporal
		 * 2: Spatial
		 * 3: Textual
		 * 
		 */
		ThreeFeatureJoin t = new ThreeFeatureJoin();
		Object[] arr = { 1, 2, 3 };
		    // 循环获得每个长度的排列组合
			for (int num = 1; num <= arr.length; num++) {
		      t.getSequence(arr, 0, num);
		    }
			
			
		for(int i = 1; i <= 3;i++){
			for(int j =1 ; j <= 3;j++){
				for(int k= 1; k <= 3;k++){
					if(i != j && j != k && i != k){
						orderList.add(i + ";" + j + ";" + k);
					}
				}
			}
		}
		
		
		for(String str : orderList){
			
			int[] order = new int[3];
			for(int i = 0; i < str.split(";").length;i++){
				order[i] = Integer.parseInt(str.split(";")[i]);
			}
			
//			System.out.println(str + ":" + FeatureTime(order));
//			System.out.println(FeatureTime(order));
			
				
		}
		System.out.println(FeatureTime(new int[]{1}));
		System.out.println(FeatureTime(new int[]{2}));
		System.out.println(FeatureTime(new int[]{3}));
		
		System.out.println("Finished");
		
	}
	
	
	public static double FeatureTime(int[] order) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		
		for (int i = 0; i < 10000; i++) {
			
			FlickrData value1 = rRecords.get(i);
			
		    for (int j = 0; j < 10000; j++) {
		    	
		    	FlickrData value2 = sRecords.get(j);
		    	
		    	int condition = 0;
		    	for(int m = 0; m < order.length; m++){
		    		
		    		if(order[m] == 1){
		    			
		    			if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
				    		firstCount++;
				    		condition++;
		    			}else{
		    				break;
		    			}
		    			
		    		}else if(order[m] == 2){
		    			
		    			if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
				    		
			    			SecondCount++;
			    			condition++;
			    			
		    			}else{
		    				break;
		    			}
		    			
		    		}else if(order[m] == 3){
		    			
		    			if (FlickrSimilarityUtil.TextualSimilarity(value1, value2)) {
		    				
		 		        	ThirdCount++;
		 		        	condition++;
		 		        	
		 		        }else{
		 		        	break;
		 		        }
		    		}
		    	}
		    	
		    	if(condition == 3){
		    				
		    		
 		            long ridA = value1.getId();
 		            long ridB = value2.getId();
 		            if (ridA < ridB) {
 		                long rid = ridA;
 		                ridA = ridB;
 		                ridB = rid;
 		            }
// 		            writer.write(ridA + "%" + ridB +"\n");
 		        }
    			
    		}
		           
		}
		
		writer.close();
		System.out.println(firstCount);
		System.out.println(SecondCount);
		System.out.println(ThirdCount);
//		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
		return (System.currentTimeMillis() -startTime)/ (float) 1000.0;
	}
}
