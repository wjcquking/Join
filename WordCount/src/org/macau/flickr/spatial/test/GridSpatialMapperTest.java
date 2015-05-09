package org.macau.flickr.spatial.test;

import java.util.ArrayList;

import org.macau.flickr.spatial.partition.GridPartition;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.spatial.Distance;

public class GridSpatialMapperTest {

	public static ArrayList<Integer> tileNumberOfR(double lat,double lon){
		
		ArrayList<Integer> list = new ArrayList<Integer>();
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		list.add(ZOrderValue.parseToZOrder(latNumber, lonNumber));
		return list;
		
	}
	
	public static ArrayList<Integer> tileOfR(double lat, double lon){
		
		ArrayList<Integer> list = new ArrayList<Integer>();
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int latNumberStart = (int) ((lat - thres - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		if(latNumberStart < 0){
			latNumberStart = 0;
		}
		int latNumberEnd = (int) ((lat + thres - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumberStart =(int)((lon-thres- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		if(lonNumberStart < 0){
			lonNumberStart = 0;
		}
		int lonNumberEnd = (int)((lon + thres - FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		
		for(int i = latNumberStart;i <= latNumberEnd;i++){
			for(int j = lonNumberStart;j <= lonNumberEnd;j++){
				list.add(ZOrderValue.parseToZOrder(i, j));
			}
		}
		
		return list;
	}
	
	/*
	 * the cell start from 0 to n-1
	 * for the S set, the tile is larger than the tile of R set one threshold width
	 */
	public static ArrayList<Integer> tileNumberOfS(double lat, double lon){
		ArrayList<Integer> list = new ArrayList<Integer>();
		double tileWidth = FlickrSimilarityUtil.wholeSpaceWidth / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		double tileHight = FlickrSimilarityUtil.WholeSpaceLength / FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
//		list.add(ZOrderValue.parseToZOrder(latNumber, lonNumber));
		
		ArrayList<Integer> latList = new ArrayList<Integer>();
		ArrayList<Integer> lonList = new ArrayList<Integer>();
		
		latList.add(latNumber);
		lonList.add(lonNumber);
		
		/*
		 * get S lat tile list
		 */
		double lat1 = FlickrSimilarityUtil.MIN_LAT + latNumber*tileWidth;
		double distance = Distance.GreatCircleDistance(lat,lon,lat1,lon);
		
		if( Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + latNumber*tileWidth,lon)< FlickrSimilarityUtil.DISTANCE_THRESHOLD ){
			if(latNumber != 0){
				latList.add(latNumber-1);
			}
		}
		
		double lat2 = FlickrSimilarityUtil.MIN_LAT + (latNumber+1)*tileWidth;
		double lat0 = FlickrSimilarityUtil.MIN_LAT + latNumber*tileWidth;
		
		double distance2 = Distance.GreatCircleDistance(lat,lon,lat2,lon);
		
		double distance0 = Distance.GreatCircleDistance(lat0,lon,lat2,lon);
		
		if(Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + (latNumber+1)*tileWidth,lon) < FlickrSimilarityUtil.DISTANCE_THRESHOLD){
			latList.add(latNumber+1);
		}
		
		
		double lon2 = FlickrSimilarityUtil.MIN_LON + lonNumber*tileHight;
		double lon0 = FlickrSimilarityUtil.MIN_LON + (lonNumber+1)*tileHight;
		
		double distancelon2 = Distance.GreatCircleDistance(lat,lon,lat,lon2);
		
		double distancelon0 = Distance.GreatCircleDistance(lat,lon0,lat,lon2);
		
		/*
		 * get S lon tile list
		 */
		if( Distance.GreatCircleDistance(lat,lon,lat,FlickrSimilarityUtil.MIN_LON + lonNumber*tileHight)< FlickrSimilarityUtil.DISTANCE_THRESHOLD ){
			if(lonNumber != 0){
				lonList.add(lonNumber-1);
			}
		}
		
		if(Distance.GreatCircleDistance(lat,lon,lat,FlickrSimilarityUtil.MIN_LON + (lonNumber+1)*tileHight) < FlickrSimilarityUtil.DISTANCE_THRESHOLD){
			lonList.add(lonNumber+1);
		}
		
		for(Integer la: latList){
			for(Integer lo: lonList){
				list.add(ZOrderValue.parseToZOrder(la, lo));
			}
		}
		
		return list;
	}
	/**
	 * 
	 * @param tileNumber
	 * @return the partition number using the round robin algorithm
	 */
	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.PARTITION_NUMBER;
		
	}
	public static void main(String[] args){
		String value = "14136087281:26847:48.847292:2.336096:1258276450000:0;119;154;230;1361;2116;2642;2746;5703;42423";
		
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		

		ArrayList<Integer> tileList = new ArrayList<Integer>();
		

		
		
			
		tileList = tileNumberOfR(lat,lon);
		System.out.println(tileList.toString());
		tileList = tileNumberOfS(lat,lon);
		
		System.out.println(tileList.toString());
		
		tileList = tileOfR(lat,lon);
		System.out.println(tileList.size());
		System.out.println(tileList.toString());

	}
}
