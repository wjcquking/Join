package org.macau.flickr.util;

import java.util.Comparator;


public class FlickrPartitionValueComparator implements Comparator{

	@Override
	public int compare(Object o1, Object o2) {
		// TODO Auto-generated method stub
		if(null == o1 || null == o2){
			return -1;
		}
		FlickrPartitionValue fpv1 = (FlickrPartitionValue) o1;
		FlickrPartitionValue fpv2 = (FlickrPartitionValue) o2;
		
		if(fpv1.getDistance() > fpv2.getDistance()){
			return 1;
		}else if(fpv1.getDistance() < fpv2.getDistance()){
			return -1;
		}else{
		
			return 0;
		}
	}

}
