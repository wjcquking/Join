package org.macau.flickr.knn.exact.preprocessing.kmeans;

/**
 * 
 * @author Yuanbo She
 *
 */
public class kmeans_param {
	public static final int CENTER_ORDER = 0;
	public static final int CENTER_RANDOM = 1;
	public static final int MAX_ATTEMPTS = 4000;
	public static final double MIN_CRITERIA = 0.00001;
	
	public double criteria = MIN_CRITERIA; // threshold
	public int attempts = MAX_ATTEMPTS;  // temp
	public int initCenterMehtod = CENTER_ORDER;  
	public boolean isDisplay = true; 
}