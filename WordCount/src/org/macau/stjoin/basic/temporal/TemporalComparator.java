package org.macau.stjoin.basic.temporal;

import java.util.Comparator;

import org.macau.flickr.util.FlickrValue;

public class TemporalComparator implements Comparator<FlickrValue>{



	@Override
	public int compare(FlickrValue o1, FlickrValue o2) {
		int flag = o1.getTimestamp() > o2.getTimestamp()?1:(o1.getTimestamp()== o2.getTimestamp()?0:-1);
		
		if(flag == 0){
			
			flag = o1.getId() > o2.getId()?1:(o1.getId()== o2.getId()?0:-1);
			
		}
		
		return flag;
	}

}
