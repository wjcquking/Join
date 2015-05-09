package org.macau.spatial;

public class Distance {
	
	 
	/**
	 * phi = lat1 - lat2, lambda = lon1 - lon2, in radians;
	 * u = sin(phi / 2), v = sin(lambda / 2);
	 * d(L1, L2) = 2arcsin(u^2 + cos(lat1)cos(lat2)v^2) * EARTH_RADIUS.
	 * Range: [0, pi * EARTH_RADIUS] ~= [0, 20037.508], in kilometers.
	 */
	public static double GreatCircleDistance ( double lat1, double lon1, double lat2, double lon2)
	{
//		double rad_lat1 = lat1 * Math.PI / 180.0;
//		double rad_lat2 = lat2 * Math.PI / 180.0;
//		double phi = rad_lat1 - rad_lat2;
//		double lambda = lon1 * Math.PI / 180.0 - lon2 * Math.PI / 180.0;
//		double u = Math.sin(phi / 2.0);
//		double v = Math.sin(lambda / 2.0);
//		double s = 2 * Math.asin(Math.sqrt(u * u + Math.cos(rad_lat1) * Math.cos(rad_lat2) * v * v));
//		return s * 6378.137;
		return Math.pow(lat1-lat2, 2) + Math.pow(lon1-lon2, 2);
	}
	
	
	/**
	 * 
	 * @param x1
	 * @param y1
	 * @param x2
	 * @param y2
	 * @return
	 * d(L1,L2) = sqrt((x1-x2)^2 + (y1-y2)^2)
	 */
	public static double EuclidDistance(double x1, double y1,double x2,double y2){
		return Math.sqrt(Math.pow(x1-x2, 2) + Math.pow(y1-y2, 2));
	}
	
	
	/**
	 * 
	 * @param x1
	 * @param y1
	 * @param x2
	 * @param y2
	 * @return
	 * d(L1,L2) = sqrt((x1-x2)^2 + (y1-y2)^2)
	 */
	public static double EuclidDistanceNoSqrt(double x1, double y1,double x2,double y2){
		return Math.pow(x1-x2, 2) + Math.pow(y1-y2, 2);
	}
	
}
