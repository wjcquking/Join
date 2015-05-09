package org.macau.stjoin.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.util.FlickrValue;

public class MapTest {

	public static void main(String[] args){
		Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		rMap.put(1, new ArrayList());
		System.out.println(rMap.containsKey(1));
	}
}
