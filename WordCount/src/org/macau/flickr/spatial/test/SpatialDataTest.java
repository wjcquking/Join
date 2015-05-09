package org.macau.flickr.spatial.test;

import org.macau.flickr.spatial.basic.BasicSpatialMapper;
import org.macau.flickr.spatial.partition.GridPartition;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.spatial.Distance;

/**
 * 
 * @author hadoop
 * For the data of Paris Flickr
 * 
 */
public class SpatialDataTest {

	public static void main(String[] args){
		/**
		 * the range of Paris flickr data
		 * latitude : 48.815101 - 48.902967, width
		 * longitude : 2.223266 - 2.473817,height
		 * 48.857952;2.414631
		 * 
		 */
		System.out.println("Distance of all the point");
		
		/*
		 * the Paris Area
		 * the height is 9.781198378041525
		 * the width is 18.366105904837223
		 * the longest distance is 20.79409087566547 
		 */
		System.out.println("the height is "+Distance.GreatCircleDistance(FlickrSimilarityUtil.MIN_LAT, FlickrSimilarityUtil.MIN_LON, FlickrSimilarityUtil.MAX_LAT, FlickrSimilarityUtil.MIN_LON));
		System.out.println("the width is "+Distance.GreatCircleDistance(FlickrSimilarityUtil.MIN_LAT, FlickrSimilarityUtil.MIN_LON, FlickrSimilarityUtil.MIN_LAT, FlickrSimilarityUtil.MAX_LON));
		System.out.println("the longest distance is "+Distance.GreatCircleDistance(FlickrSimilarityUtil.MIN_LAT, FlickrSimilarityUtil.MIN_LON, FlickrSimilarityUtil.MAX_LAT, FlickrSimilarityUtil.MAX_LON));
		
		System.out.println(GridPartition.tileNumber(48.902967, 2.414631));
		
		/*
		 * Caculate two objects distance
		 * 
		 */
		
		String One = "3820596202;48.88655;2.340581;1249438230000";
		String One1 = "3820596202;48.88655;2.340581;1249438230000";
		String Two = "2595020925;48.886536;2.340592;1213429970000";
		
		System.out.println("the height is "+Distance.GreatCircleDistance(48.88655, 2.340581, 48.886568,2.340581));
	}
}
