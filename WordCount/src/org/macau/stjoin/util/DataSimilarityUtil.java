package org.macau.stjoin.util;

import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.FlickrValueWithMultiFeature;

public class DataSimilarityUtil {

	public static void getFlickrValue(FlickrValue outputValue, String value){
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		String textual = value.toString().split(":")[5];
		String others = value.toString().split(":")[6];
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		outputValue.setTiles(textual);
		outputValue.setOthers(others);
	}
	
	public static void getFlickrValue(FlickrValueWithMultiFeature outputValue, String value){
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		String textual = value.toString().split(":")[5];
		String others = value.toString().split(":")[6];
		
		
		String candidateTags = "";
		long uploadtime = Long.parseLong(value.toString().split(":")[1]);
		long serverId = Long.parseLong(value.toString().split(":")[7]);;
		String device = value.split(":")[8];
		String description = value.split(":")[8];
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		outputValue.setTiles(textual);
		outputValue.setOthers(others);
		outputValue.setCandidateTags(candidateTags);
		outputValue.setTileTag("");
		outputValue.setUploadtime(uploadtime);
		outputValue.setServerId(serverId);
		outputValue.setDevice(device);
		outputValue.setDescription(description);
	}
	
	
	public static FlickrValue getFlickrValue(String value){
		
		FlickrValue outputValue = new FlickrValue();
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		return new FlickrValue(outputValue);
	}
}
