package org.macau.flickr.knn.exact.first;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.macau.flickr.knn.util.kNNUtil;

public class ParitioinTest {

	public static kNNPartition[] R_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	public static void main(String[] args){
		
		/*
		 * test the partition function
		 */
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			kNNPartition kp = new kNNPartition();
			kp.setCount(0);
			R_Partition[i] = kp;
			System.out.println(R_Partition.length);
			System.out.println(R_Partition[i].getCount());
			R_Partition[i].setCount(0);
		}
		
		Map<Integer, Double> keyfreqs = new HashMap<Integer, Double>();
		
		for(int i = 0; i< 10;i++){
			keyfreqs.put(i, (double) (10.1-i));
		}

		ArrayList<Entry<Integer,Double>> l = new ArrayList<Entry<Integer,Double>>(keyfreqs.entrySet());  
		
		Collections.sort(l, new Comparator<Map.Entry<Integer, Double>>() {  
			
			public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {  
				return Double.compare(o1.getValue(), o2.getValue());  
			}  
			
		});
		
		for(Entry<Integer,Double> e : l) {
			System.out.println(e.getKey() + "::::" + e.getValue());
		}
	}
}
