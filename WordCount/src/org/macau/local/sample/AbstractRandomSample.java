package org.macau.local.sample;

import java.util.List;
import java.util.Random;

public abstract class AbstractRandomSample {

	/**
	 * 
	 * @param r : the sample size
	 * @param list
	 * @return One Type of Data
	 */
	public static <T>T BlackBoxU2(int r,List<T> list){
		
		int N = 0;
		int i = 0;
		T t = null;
		
		for(T fd: list){
			N += 1;
			Random random = new Random();
			
			if(random.nextDouble() <= 1D/N){
				i = N -1 ;
			}
		}
		
		if(list.size() > 0){
			
			t = list.get(i);
			
		}else{
			
		}
		
		return t;
	}
	
}
