package org.macau.stjoin.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class MapTest {

	
	public boolean hasCommonCandidateTag(String str1, String str2){
		List<String> itext = new ArrayList<String>(Arrays.asList(str1.split("#")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(str2.split("#")));
		
		boolean result = true;
		for(int i =0; i < jtext.size();i++){
			if(!hasCommonTag(itext.get(i),jtext.get(i))){
				result = false;
				break;
			}
		}
		
		return result;
	}
	
	public boolean hasCommonTag(String str1, String str2){
		
		List<String> itext = new ArrayList<String>(Arrays.asList(str1.split(";")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(str2.split(";")));
		
		jtext.retainAll(itext);
//		int numOfIntersection = jtext.size();
		
		return jtext.size() >0 ? true :false;
	}
	
	
	public static void main(String[] args){
		String value = "3924433288:152270:48.865519:2.323737:1253025551000:0;97;99:AAAAAAAAAAAAA";
		FlickrValue value1 = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));
		FlickrValue value2 = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));

	}
}
