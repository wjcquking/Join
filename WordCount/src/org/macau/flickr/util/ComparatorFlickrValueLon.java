package org.macau.flickr.util;

import java.util.Comparator;

public class ComparatorFlickrValueLon implements Comparator{

	@Override
	public int compare(Object arg0, Object arg1) {
		// TODO Auto-generated method stub
		FlickrValue fv0 = (FlickrValue) arg0;
		FlickrValue fv1 = (FlickrValue) arg1;
		
		int flag = fv0.getLon() > fv1.getLon()?1:(fv0.getLon()== fv1.getLon()?0:-1);
		if(flag == 0){
			flag = fv0.getId() > fv1.getId()?1:(fv0.getId()== fv1.getId()?0:-1);
		}
		return flag;
	}

}
