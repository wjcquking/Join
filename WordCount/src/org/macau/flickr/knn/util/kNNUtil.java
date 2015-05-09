package org.macau.flickr.knn.util;

import org.macau.flickr.util.FlickrSimilarityUtil;

public class kNNUtil {

	//kNN number
	public static final int k = 20;
	
	/**
	 * exact kNN
	 */
	//reducer number
	public static final int REDUCER_NUMBER = 1000;

	public static final int PARTITION_NUMBER = 1000;
	
	public static final double probability = 0.1;
	
	//produce the different pivot list
	public static final String  R_FILE_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.even.data";
	public static final String R_RANDOM_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.random.data";
	public static final String R_FARTHEST_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.farthest.data";
	public static final String R_KMEANS_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.kmeans.data";
	
	//the pivot file position in the DFS
	public static final String pivotOutputPath = "hdfs://localhost:9000/user/hadoop/pivot";
	public static final String summaryTablePath = "hdfs://localhost:9000/user/hadoop/summary";
	
	
	//the information get from the data
	public static final String R_InformationPart = "hdfs://localhost:9000/user/hadoop/r_information";
	public static final String S_InformationPart = "hdfs://localhost:9000/user/hadoop/s_information";
	
	//random selection
	public static final int RANDOM_SELECTION_T = 20;
	
	public static final double RANDOM_SELECTION_PROBABILITY = 0.005;
	
	
	//farthest selection
	public static final double FARTHEST_SELECTION_PROBABILITY = 0.0005;
	
	//k-means selection
	public static final double KMEANS_SELECTION_PROBABILITY = 0.005;
	
	/**
	 * approximate kNN
	 */
	//approximate util value
	// the vector number
	public static int alpha = 5;
	

	
	//calculate the sample probability which equal = 1/(epsilon^2 * N)
	public static double epsilon = 0.01;
}
