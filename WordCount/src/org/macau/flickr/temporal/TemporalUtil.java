package org.macau.flickr.temporal;


/**
 * 
 * @author hadoop
 * processing the data and get the information of all the data
 */
public class TemporalUtil {
	//2012-4-20
	public static long maxTimestamp = 1334917082000L;
	
	//-1167638400000
	//1990-3-1
	public static long minTimestamp = 636220800000L;
	
	public static final  long MS_OF_ONE_DAY = 86400000;
	
	//one week
	public static final long TIME_PERIOD = 7 * MS_OF_ONE_DAY;
}
