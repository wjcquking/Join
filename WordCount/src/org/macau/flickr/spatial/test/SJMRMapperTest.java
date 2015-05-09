package org.macau.flickr.spatial.test;

import org.macau.flickr.spatial.sjmr.SJMRSpatialMapper;
import org.macau.flickr.util.FlickrValue;

public class SJMRMapperTest {

	public static void main(String[] args){
		//65480044:48.874651;2.333221;1131126026000;6287
		String value = "3789402015;48.856244;2.354164;1220007541000";
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		FlickrValue outputValue = new FlickrValue();
		int  tileNumber = SJMRSpatialMapper.tileNumber(lat,lon);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tileNumber);
		outputValue.setTimestamp(timestamp);
		
		System.out.println(SJMRSpatialMapper.paritionNumber(tileNumber));
		System.out.println(outputValue);
	}
}
