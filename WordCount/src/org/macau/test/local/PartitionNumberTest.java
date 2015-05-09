package org.macau.test.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class PartitionNumberTest {

	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.PARTITION_NUMBER;
		
	}
	
	public static void main(String[] args){
		System.out.println(paritionNumber(2));
		Map<Integer,String> map = new HashMap<Integer,String>();
		
		map.put(12,"1");
		System.out.println(map.containsKey(12));
	}
}
