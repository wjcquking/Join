package org.macau.flickr.spatial.partition;

import java.util.ArrayList;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.spatial.Distance;

public class GridPartition {

	/*
	 * use the z curve order and round-robin algorithm to find the best partition function
	 * the universe is divided regularly into Nt tiles
	 * each tile is number from 0 to Nt-1 according to z curve, 
	 * and mapped to a partition p with a round robin scheme
	 * 
	 */

	
	public static int tileNumber(double lat,double lon){
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		return ZOrderValue.parseToZOrder(latNumber, lonNumber);
		
	}
	
	/*
	 * use the round robin algorithm to make the reducer work load almost even
	 */
	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.PARTITION_NUMBER;
		
	}
	
	/**
	 * 
	 * @param lat
	 * @param lon
	 * @return the overlap tile list
	 * 
	 */
	public static ArrayList<Integer> tileNumberByMultiAssignment(double lat, double lon){
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
		if( Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + latNumber*tileWidth,lon)< FlickrSimilarityUtil.DISTANCE_THRESHOLD ){
			if(latNumber != 0){
				latList.add(latNumber-1);
			}
		}
		
		if(Distance.GreatCircleDistance(lat,lon,FlickrSimilarityUtil.MIN_LAT + (latNumber+1)*tileWidth,lon) < FlickrSimilarityUtil.DISTANCE_THRESHOLD){
			latList.add(latNumber+1);
		}
		
		
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
	
}
