package org.macau.flickr.spatial.sjmr;

import org.macau.flickr.util.FlickrSimilarityUtil;

/**
 * 
 * @author hadoop
 * the function of this class is 
 * First,the universe is divided regularly into Nt tiles
 * Second: each tile is number from 0 to Nt-1 according to z curve, and mapped to a partition p with a round robin scheme
 */

public class SpatialPartitioningFunction {
	
	public static int tilesTotalNumber = FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
	
}
