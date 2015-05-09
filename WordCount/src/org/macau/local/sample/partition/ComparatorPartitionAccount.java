package org.macau.local.sample.partition;

import java.util.Comparator;

public class ComparatorPartitionAccount implements Comparator{

	@Override
	public int compare(Object arg0, Object arg1) {
		// TODO Auto-generated method stub
		
		PartitionAccount p1 = (PartitionAccount)arg0;
		PartitionAccount p2 = (PartitionAccount)arg1;
		
		int flag = new Double(p2.getProduct()).compareTo(p1.getProduct());
		return flag;
	}
	
}
