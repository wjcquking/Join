package org.macau.local.util;

import java.util.Comparator;

public class ComparableWeightData implements Comparator{

	@Override
	public int compare(Object arg0, Object arg1) {
		// TODO Auto-generated method stub
		return (int) (((WeightData)arg0).getKey() - ((WeightData)arg0).getKey());
	}



}
