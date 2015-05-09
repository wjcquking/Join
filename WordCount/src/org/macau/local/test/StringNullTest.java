package org.macau.local.test;

import java.util.HashSet;
import java.util.Set;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.util.FlickrData;

public class StringNullTest {
	public static void main(String[] args){
		
		FlickrData a = new FlickrData();
		a.setTextual("null");
		FlickrData b = new FlickrData();
		b.setTextual("null");
		Set<FlickrData> set = new HashSet<FlickrData>();
		set.add(a);
		set.add(b);
		System.out.println(set.size());
		
		System.out.println(FlickrSimilarityUtil.TextualSimilarity(a,b));
		
		String str = "null";
		System.out.println(!str.equals("null"));
		

		int[] iterationCount = new int[10];
		for(int i = 0; i < 10;i++){
			iterationCount[i]++;
		}
		for(int i : iterationCount){
			
			System.out.println(i);
		}
		
				
	}
}
