package org.macau.stjoin.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.macau.flickr.util.FlickrValue;

public class SpatialTextualJoinUtil {

	public static FlickrValue getFlickrValue(String value){
		
		FlickrValue outputValue = new FlickrValue();
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[2]);
		double lon = Double.parseDouble(value.toString().split(";")[3]);
		long timestamp = Long.parseLong(value.toString().split(";")[4]);
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		return outputValue;
	}
	
	public static double getTokenSimilarity(String iToken,String jToken){
		
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(";")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(";")));
		
		int i_num = itext.size();
		int j_num = jtext.size();
		
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
}
