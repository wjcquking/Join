package org.macau.local.sample;

import java.util.List;
import java.util.Random;

public class SampleUtil {

	/**
	 * 
	 * @param r
	 * @param list
	 * @return A FlickrData
	 */
	public static <T>T BlackBoxU2(int r,List<T> list){
		
		int N = 0;
		int i = 0;
		for(T fd: list){
			N += 1;
			Random random = new Random();
			
			if(random.nextDouble() <= 1D/N){
				i = N-1;
			}
		}
		if(list.size() > 0){
			return list.get(i);
			
		}else{
			return null;
		}
	}
}
