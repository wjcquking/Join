package org.macau.local.util;

import java.util.Comparator;


public class WeightDataComparator implements Comparator{

	@Override
	public int compare(Object o1, Object o2) {
		// TODO Auto-generated method stub
		if(null == o1 || null == o2){
			return -1;
		}
		WeightData wd1 = (WeightData) o1;
		WeightData wd2 = (WeightData) o2;
		
		if(wd1.getKey() > wd2.getKey()){
			return 1;
		}else if(wd1.getKey() < wd2.getKey()){
			return -1;
		}else{
			return 0;
		}
	}
}
