package org.macau.flickr.job;

import org.apache.hadoop.conf.Configuration;
import org.macau.flickr.knn.exact.first.PartitionJob;
import org.macau.flickr.knn.exact.second.kNNJoinJob;
import org.macau.flickr.spatial.analysis.SpatialAccount;
import org.macau.flickr.spatial.basic.BasicSpatialRSSimilarityJoin;
import org.macau.flickr.spatial.grid.GridSpatialSimilarityJoin;
import org.macau.flickr.spatial.minimal.MiniSpatialSort;
import org.macau.flickr.spatial.minimal.MiniSpatialSample;
import org.macau.flickr.spatial.sjmr.SJMRSpatialSimilarityJoin;
import org.macau.flickr.spatial.sjmr.rs.SJMRSpatialRSSimilarityJoin;
import org.macau.flickr.temporal.analysis.TemporalAccount;
import org.macau.flickr.temporal.basic.TemporalJoinMapper;
import org.macau.stjoin.basic.temporal.TemporalJoinJob;

public class FlickrSimilarityJoin {

	public static int[][][] bounds = new int[5][30][3];
	public static int[][] bound = new int[30][3];
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		//FileSystem.get(conf).delete(new Path(FlickrSimilarityUtil.flickrOutputPath), true);
		Long startTime = System.currentTimeMillis();
		System.out.println("The Start time is " + startTime);
		
		long milliSeconds = 1000*60*60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		
		boolean state = false;
//		state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
		
		//1
		bounds[0] =new int[][] {{1056,2114,112},{1425,2114,112},{1450,2114,112},{1487,2114,112},{1494,2114,112},{1503,2114,112},{1512,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1829,2114,112},{1885,2114,112},{1915,2114,112},{1941,2114,112},{1955,2114,112},{1968,2114,112},{1984,2114,112},{1996,2114,112},{2007,2114,112},{2019,2114,112},{2033,2114,112},{2047,2114,112},{2055,2114,112},{2064,2114,112},{2075,2114,112},{2090,2114,112},{2106,2114,112},{2122,2114,112}};
		
		
		//2
		bounds[1] =new int[][]{{1056,2114,112},{1494,2114,112},{1503,2114,112},{1512,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1779,2114,112},{1876,2114,112},{1911,2114,112},{1932,2114,112},{1945,2114,112},{1955,2114,112},{1968,2114,112},{1985,2114,112},{1996,2114,112},{2003,2114,112},{2009,2114,112},{2018,2114,112},{2028,2114,112},{2041,2114,112},{2051,2114,112},{2058,2114,112},{2065,2114,112},{2073,2114,112},{2088,2114,112},{2105,2114,112},{2122,2114,112}};
		
		
		//3
		bounds[2] =new int[][] {{1056,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1779,2114,112},{1876,2114,112},{1911,2114,112},{1932,2114,112},{1946,2114,112},{1955,2114,112},{1964,2114,112},{1976,2114,112},{1988,2114,112},{1995,2114,112},{2003,2114,112},{2009,2114,112},{2014,2114,112},{2018,2114,112},{2025,2114,112},{2036,2114,112},{2047,2114,112},{2053,2114,112},{2059,2114,112},{2065,2114,112},{2071,2114,112},{2079,2114,112},{2090,2114,112},{2106,2114,112}};
		
		
		//4
		bounds[3] =new int[][] {{1056,2114,112},{1599,2114,112},{1605,2114,112},{1829,2114,112},{1893,2114,112},{1920,2114,112},{1941,2114,112},{1949,2114,112},{1956,2114,112},{1964,2114,112},{1974,2114,112},{1984,2114,112},{1991,2114,112},{1996,2114,112},{2003,2114,112},{2008,2114,112},{2012,2114,112},{2018,2114,112},{2024,2114,112},{2033,2114,112},{2041,2114,112},{2049,2114,112},{2054,2114,112},{2058,2114,112},{2062,2114,112},{2068,2114,112},{2071,2114,112},{2079,2114,112},{2090,2114,112},{2107,2114,112}};
		
		
		//5
		bounds[4] =new int[][]{{1056,2114,112},{1856,2114,112},{1903,2114,112},{1928,2114,112},{1945,2114,112},{1951,2114,112},{1958,2114,112},{1966,2114,112},{1975,2114,112},{1984,2114,112},{1992,2114,112},{1997,2114,112},{2003,2114,112},{2008,2114,112},{2012,2114,112},{2017,2114,112},{2023,2114,112},{2029,2114,112},{2037,2114,112},{2044,2114,112},{2050,2114,112},{2055,2114,112},{2059,2114,112},{2062,2114,112},{2068,2114,112},{2072,2114,112},{2079,2114,112},{2088,2114,112},{2099,2114,112},{2109,2114,112}};
		
		FlickrSimilarityJoin.bound = FlickrSimilarityJoin.bounds[Integer.parseInt(args[2])];
		
		
		if(args[0].equals("temporal")){
			
			state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf,Integer.parseInt(args[1]));
			
		}else if(args[0].equals("ego1")){
			
			state = org.macau.stjoin.group.GroupStatisticsJob.TemporalSimilarityBasicJoin(conf,Integer.parseInt(args[1]));
			
		}else if(args[0].equals("ego2")){
			
			state = org.macau.stjoin.ego.SuperEGOJoinJob.TemporalSimilarityBasicJoin(conf,Integer.parseInt(args[1]));
			
		}else if(args[0].equals("count")){
			
			state = org.macau.stjoin.count.phase1.TemporalCountJob.TemporalSimilarityBasicJoin(conf,Integer.parseInt(args[1]));
			
		}else if(args[0].equals("egooptimal")){
			
			
			
			state = org.macau.stjoin.group.join.TemporalJoinJob.TemporalSimilarityBasicJoin(conf,Integer.parseInt(args[1]));
			
			
			
		}
		
//		boolean state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = TemporalAccount.TemporalAccountJob(conf);
//		boolean state = SpatialAccount.spatialAccountJob(conf);
//		boolean state = SJMRSpatialSimilarityJoin.SJMRSpatialJoin(conf);
		
//		boolean state = SpatialSimilarityJoin.SpatialSimilarityBasicJoin(conf);
//		boolean state = TemporalSimilarityJoin.TemporalSimilarityBasicJoin(conf);
		
		//the spatial RS similairty Join
//		boolean state = SJMRSpatialRSSimilarityJoin.SJMRSpatialJoin(conf);
//		boolean state = GridSpatialSimilarityJoin.GridSpatialJoin(conf);
//		boolean state = MiniSpatialSample.MiniSpatial(conf);
//		boolean state = MiniSpatialSort.MiniSpatial(conf);
		
//		boolean state = BasicSpatialRSSimilarityJoin.BasicSpatialJoin(conf);
		
		/*******************************************
		 * 
		 * Count Method
		 * 
		 *******************************************/
//		state = org.macau.stjoin.count.phase1.TemporalCountJob.TemporalSimilarityBasicJoin(conf, 1);		
		
		/******************
		 * GrouopStatistics use only one mapper sort the data
		 * SuperEGO 
		 *****************/
//		boolean state = org.macau.stjoin.group.GroupStatisticsJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.ego.SuperEGOJoinJob.TemporalSimilarityBasicJoin(conf);
		
		
		
//		boolean state = org.macau.stjoin.group.join.TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
		
		/*
		 * the kNN spatial Join
		 */
//		boolean state = PartitionJob.spatialPartitionjob(conf);
//		
//		boolean state = org.macau.stjoin.basic.spatial.threshold.GridSpatialThresholdSimilarityJoin.GridSpatialJoin(conf);
//		boolean state = org.macau.stjoin.basic.temporal.combination.threshold.TemporalSpatialCombinationJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.basic.temporal.mixture.threshold.TemporalSpatialMixtureJoinJob.TemporalSimilarityBasicJoin(conf);
		
		
//		boolean state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.basic.spatial.GridSpatialSimilarityJoin.GridSpatialJoin(conf);
		
		
		
//		boolean state = org.macau.stjoin.basic.temporal.combination.TemporalSpatialCombinationJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.basic.temporal.mixture.TemporalSpatialMixtureJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.basic.textual.TextualJoinJob.TextualSimilarityBasicJoin(conf);
		
		if(state){
			System.out.println("End at " + System.currentTimeMillis());
			System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
		}
		
//		boolean second = kNNJoinJob.spatialPartitionjob(conf);
//		if(second){
//			System.out.println("Phase Two cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
//		}
	}
}