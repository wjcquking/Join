package org.macau.local.mfeat;

import java.util.List;

public class MFeatUtil {

	/*
	 * 1. mfeat-fou: 76 Fourier coefficients of the character shapes; 
	 * 2. mfeat-fac: 216 profile correlations; 
	 * 3. mfeat-kar: 64 Karhunen-Love coefficients;
	 * 4. mfeat-pix: 240 pixel averages in 2 x 3 windows; 
	 * 5. mfeat-zer: 47 Zernike moments; 
	 * 6. mfeat-mor: 6 morphological features. 
	 * 
	 */
	public final static String mFeatFou ="D:\\Data\\mfeat\\mfeat-fou";
	public final static String mFeatFac ="D:\\Data\\mfeat\\mfeat-fac";
	public final static String mFeatKar ="D:\\Data\\mfeat\\mfeat-kar";
	public final static String mFeatPix ="D:\\Data\\mfeat\\mfeat-pix";
	public final static String mFeatZer ="D:\\Data\\mfeat\\mfeat-zer";
	public final static String mFeatMor ="D:\\Data\\mfeat\\mfeat-mor";
	
	public final static double fouThreshold = 1;
	public final static double facThreshold = 1000000;
	public final static double karThreshold = 700;
	public final static double pixThreshold = 3000;
	public final static double zerThreshold = 500000;
	public final static double morThreshold = 5000000;
	
	
	
	public static double getSimilarityValue(List<Double> record1, List<Double> record2){
		double result = 0;
		for(int i = 0; i < record1.size();i++){
			result += Math.pow(record1.get(i)-record2.get(i), 2);
		}
		return result;
	}
}
