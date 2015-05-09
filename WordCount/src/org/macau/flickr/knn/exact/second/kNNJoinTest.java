package org.macau.flickr.knn.exact.second;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrPartitionValueComparator;

public class kNNJoinTest {
	public static void main(String[] args){
		System.out.println("the kNN test");
		List<FlickrPartitionValue> topList = new ArrayList<FlickrPartitionValue>();
		topList.add(new FlickrPartitionValue(1,1,1,1));
		topList.add(new FlickrPartitionValue(2,1,3,2));
		topList.add(new FlickrPartitionValue(1,1,2,3));
		
		Collections.sort(topList,new FlickrPartitionValueComparator());
		
		for(FlickrPartitionValue f: topList){
			System.out.println(f);
		}
	}
}
