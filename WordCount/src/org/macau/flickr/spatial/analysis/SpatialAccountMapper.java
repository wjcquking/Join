package org.macau.flickr.spatial.analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;

public class SpatialAccountMapper extends
	Mapper<Object,Text,DoubleWritable,IntWritable>{
	
	private final DoubleWritable outputKey = new DoubleWritable();
	
	private final static IntWritable one = new IntWritable(1);

	public static int tileNumber(double lat,double lon){
		
//		System.out.println(lat - FlickrSimilarityUtil.minLat);
//		System.out.println((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth);
//		System.out.println((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		return latNumber + lonNumber* FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
	}
	
	public void map(Object key,Text value,Context context)
		throws IOException, InterruptedException{
		
		/**
		 * the range of Paris flickr data
		 * latitude : 48.815101 - 48.902967
		 * longitude : 2.223266 - 2.473817
		 * 
		 */
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		
		double timestamp = Double.parseDouble(value.toString().split(":")[4]);
		
		int day = (int)(timestamp/(3600*1000*24));
		outputKey.set(lat);
		
//		if(tileNumber(lat,lon) == 149){
//			System.out.println(value);
//		}
		
		context.write(outputKey,one);
	}
}
