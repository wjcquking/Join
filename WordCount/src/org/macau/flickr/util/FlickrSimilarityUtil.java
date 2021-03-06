package org.macau.flickr.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.macau.local.util.FlickrData;
import org.macau.spatial.Distance;

public class FlickrSimilarityUtil {

	//time threshold
	// the "L" is very important
////	public static final long TEMPORAL_THRESHOLD = 700L*86400000L;
//	public static final long TEMPORAL_THRESHOLD = 10L*86400000L + 2L*8640000L + 2L*864000L + 0L*86400L;
////	public static final long TEMPORAL_THRESHOLD = 1L*2592000L;
//		
////	public static final double DISTANCE_THRESHOLD = 0.0001;
//	public static final double DISTANCE_THRESHOLD = 0.0000031;
//	//textual threshold
//	public static final double TEXTUAL_THRESHOLD = 0.143;
	
	//0.005
	public static final long TEMPORAL_THRESHOLD = 5L*86400000L + 1L*8640000L + 1L*864000L + 0L*86400L;
//	public static final double DISTANCE_THRESHOLD = 0.00000075;
	public static final double TEXTUAL_THRESHOLD = 0.141;
	
	//0.001
//	public static final long TEMPORAL_THRESHOLD = 0L*86400000L + 9L*8640000L + 5L*864000L + 0L*86400L;
//	public static final double DISTANCE_THRESHOLD = 0.000000047;
//	public static final double TEXTUAL_THRESHOLD = 0.32;

	//0.0005
//	public static final long TEMPORAL_THRESHOLD = 0L*86400000L + 4L*8640000L + 4L*864000L + 0L*86400L;
	public static final double DISTANCE_THRESHOLD = 0.000000017;
//	public static final double TEXTUAL_THRESHOLD = 0.45;
	
	//0.0001
//	public static final long TEMPORAL_THRESHOLD = 0L*86400000L + 0L*8640000L + 7L*864000L + 0L*86400L;
//	public static final double DISTANCE_THRESHOLD = 0.0000000023;
//	public static final double TEXTUAL_THRESHOLD = 0.8;
	
	
	public static final int loop = 2;
	
	//the other threshold
	public static final long UPLOAD_THRESHOLD = 1L*86400L;
	public static final long SERVER_THRESHOLD = 10;
	public static final double DEVICE_THRESHOLD = 0.5;
	public static final double DESCRIPTION_THRESHOLD = 0.5;
	
	
	public static final double SAMPLE_PROBABILITY = 0.004;
	
	public static final int MACHINE_NUNBER = 3;
	//three machine and each has two reduces
	public static final int REDUCER_NUMBER = 100;
	
	//the tile number of each line
	public static final int TILE_NUMBER_EACH_LINE = 10; 
	
	public static final int TOTAL_TILE_NUMBER = TILE_NUMBER_EACH_LINE * TILE_NUMBER_EACH_LINE;
	
	public static final int PARTITION_NUMBER = 30;
	
	/* For the data of Paris flickr image picture
	 * If We know the data,we can split the whole universe
	*/
	public static final double MAX_LAT = 48.902967;
	public static final double MAX_LON = 2.473817;
	public static final double MIN_LAT = 48.815101;
	public static double MIN_LON = 2.223266;
	
	public static double wholeSpaceWidth = MAX_LAT - MIN_LAT;
	public static double WholeSpaceLength = MAX_LON - MIN_LON;
	
	public static final int R_tag = 0;
	public static final int S_tag = 1;
	
	public static final String R_TAG = "even";
	public static final String S_TAG = "odd";
	
	
	public static final String Spatial_TAG = "S:";
	public static final String Temporal_TAG = "T:";
	
//	public static final String flickrInputPath = "hdfs://localhost:9000/user/hadoop/input";
//	public static final String flickrOutputPath = "hdfs://localhost:9000/user/hadoop/output";
//	public static final String flickrResultPath = "hdfs://localhost:9000/user/hadoop/result";
//	public static final String flickrInputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/input";
//	public static final String flickrOutputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/output";
//	
	public static final String flickrInputPath = "hdfs://fireslate.cis.umac.mo:8025/user/mb25428/input2";
	public static final String flickrOutputPath = "hdfs://fireslate.cis.umac.mo:8025/user/mb25428/output";
	public static final String flickrResultPath = "hdfs://fireslate.cis.umac.mo:8025/user/mb25428/result";
	
//	public static final String flickrInputPath = "hdfs://compute-n:8025/user/hadoop/input2";
//	public static final String flickrOutputPath = "hdfs://compute-n:8025/user/hadoop/output";
//	public static final String flickrResultPath = "hdfs://compute-n:8025/user/hadoop/result";
	
	/**
	 * 
	 * @param value1
	 * @param value2d
	 * @return if satisfy the similarity threshold, then return true, else return false
	 */
	public static boolean TemporalSimilarity(FlickrValue value1,FlickrValue value2){
		
		return Math.abs(value1.getTimestamp()- value2.getTimestamp()) < TEMPORAL_THRESHOLD;
		
	}
	
	
	public static boolean SingleValueSimilarity(long value1,long value2, double threshold){
		return Math.abs(value1 - value2) < threshold;
	}
	
	public static boolean MultiValueSimilarity(String value1, String value2,String separate,double threshold){
		return getTokenSimilarity(value1, value2, separate) > threshold;
	}
	
	public static int getTagByFileName(String fileName){
		
		if(fileName.contains(R_TAG)){
			
			return R_tag;
			
		}else{
			return S_tag;
		}
		
	}
	
	
	public static boolean TemporalSimilarity(FlickrData value1,FlickrData value2){
		
		return Math.abs(value1.getTimestamp()- value2.getTimestamp()) < TEMPORAL_THRESHOLD;
		
	}
	
	public static double getTokenSimilarity(String iToken,String jToken,String separate){
		
//		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(";")));
//		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(";")));
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(separate)));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(separate)));
		
		int i_num = itext.size();
		int j_num = jtext.size();
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
	
	
	/**
	 * 
	 * @param iToken
	 * @param jToken
	 * @return the similarity value of two tokens
	 * 
	 */
	public static double getTokenSimilarity(String iToken,String jToken){
		
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(";")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(";")));
//		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(",")));
//		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(",")));
		
		int i_num = itext.size();
		int j_num = jtext.size();
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
	
	/**
	 * 
	 * @param i lat:lon
	 * @param j lat:lon
	 * @return the distance of two point
	 */
	
	public static double getDistance(String i, String j){
		double iLat = Double.parseDouble(i.split(":")[0]); 
		double iLon = Double.parseDouble(i.split(":")[0]);
		double jLat = Double.parseDouble(i.split(":")[0]);
		double jLon = Double.parseDouble(j.split(":")[0]);
		return Distance.GreatCircleDistance(iLat,iLon,jLat,jLon);
	}
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return if the distance of two objects is larger than the distance threshold, return true, else return false
	 */
	public static boolean SpatialSimilarity(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon()) < DISTANCE_THRESHOLD;
	}
	
	public static boolean SpatialSimilarity(FlickrData value1, FlickrData value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon()) < DISTANCE_THRESHOLD;
	}
	
	public static boolean TextualSimilarity(FlickrValue value1, FlickrValue value2){
		return getTokenSimilarity(value1.getTiles(), value2.getTiles()) > TEXTUAL_THRESHOLD;
	}
	
	public static boolean TextualSimilarity(FlickrData value1, FlickrData value2){
		return getTokenSimilarity(value1.getTextual(), value2.getTextual()) > TEXTUAL_THRESHOLD;
	}
	
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return the spatial distance between two FlickrValue
	 */
	public static double SpatialDistance(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon());
	}
	
	/**
	 * 
	 * @param value which read in the raw file
	 * @return the FlickrValue which gets from the String value
	 */
	public static FlickrValue getFlickrVallueFromString(String value){
		
		FlickrValue outputValue = new FlickrValue();
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTiles(value.toString().split(":")[5]);
		outputValue.setTimestamp(timestamp);
		
		return outputValue;
	}
	
	/**
	 * 
	 * @param value which read in the raw file
	 * @return the FlickrValue which gets from the String value
	 */
	public static FlickrData getFlickrDataFromString(String value){
		
		FlickrData outputValue = new FlickrData();
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTextual(value.toString().split(":")[5]);
		outputValue.setTimestamp(timestamp);
		
		return outputValue;
	}
	
	
	/***********************************************
	 * 
	 *  Write the output of console to the file
	 *  
	 ***********************************************/
	public static void writeConsoleToFile(){
		
		try {
			
			System.setOut(new PrintStream(new FileOutputStream("D:\\output.txt")));
			
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static String toFullBinaryString(int num){
		char[] chs = new char[Integer.SIZE];
		for(int i = 0; i < Integer.SIZE;i++){
			chs[Integer.SIZE-1-i] = (char)(((num >> i) & 1) + '0');
		}
		return new String(chs);
	}
	/**
	 * Find the z curve order (=vertex index) for the given grid cell 
	 * coordinates.
	 * @param x cell column (from 0)
	 * @param y cell row (from 0)
	 * @param r resolution of z curve (grid will have Math.pow(2,r) 
	 * rows and cols)
	 * @return z order value
	 */
	public static long parseToZOrder(int x, int y){
		
		String xString = toFullBinaryString(x);
		String yString = toFullBinaryString(y);
		String z = "";
		char[] xChar = xString.toCharArray();
		char[] yChar = yString.toCharArray();
		for(int i = 0; i < xChar.length;i++){
			z += yChar[i];
			z += xChar[i];
			
		}
		return Long.parseLong(z,2);
	}
}