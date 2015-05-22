package org.macau.stjoin.ego.optimal.index;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValueWithCandidateTags;
import org.macau.stjoin.util.DataSimilarityUtil;
import org.macau.util.SimilarityUtil;

public class EGOOptimalWithReducerIndexMapper extends
	Mapper<Object, Text, LongWritable, FlickrValueWithCandidateTags>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final FlickrValueWithCandidateTags outputValue = new FlickrValueWithCandidateTags();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("EGO index mapper Start at " + System.currentTimeMillis());
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		DataSimilarityUtil.getFlickrValue(outputValue, value.toString());
		
//		long id =Long.parseLong(value.toString().split(":")[0]);
//		double lat = Double.parseDouble(value.toString().split(":")[2]);
//		double lon = Double.parseDouble(value.toString().split(":")[3]);
//		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = outputValue.getTimestamp() / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		outputValue.setTileNumber((int)timeInterval);
		
//		outputValue.setId(id);
//		outputValue.setLat(lat);
//		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
//		outputValue.setTiles(value.toString().split(":")[5]);
//		outputValue.setOthers(value.toString().split(":")[6]);
//		
//		
//		//Add the candidate tags
//		String textual = value.toString().split(":")[5];
		
		
		String textualTag = "";
		
		if(!outputValue.getTiles().equals("null")){
			
			String[] textualList = outputValue.getTiles().split(";");
			
			
			//get the prefix values
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			//for(int i = 0; i < prefixLength;i++){
			for(int i = textualList.length-1; i >= textualList.length - prefixLength;i--){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				if(i == textualList.length - prefixLength){
					textualTag += tokenID;
				}else{
					textualTag += tokenID + ",";
				}
			}
			
		}else{
			textualTag = " ";
		}
		
		String spatialTag = "";
		String temporalTag = "";
		
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int x = (int) (outputValue.getLat() /thres);
		int y = (int)(outputValue.getLon()/thres );
			
		spatialTag =  x + ":" + y;
		temporalTag = timeInterval + "";
			
		
		String candidateTags = temporalTag + "#"+ spatialTag + "#"+textualTag;
		
//		outputValue.setTimestamp(timestamp);
		outputValue.setCandidateTags(candidateTags);
		
		//for the R data
//		int [][] bounds= {{190,2444,119},{195,2444,119,},{199,2444,118},{205,2444,118}};
		
		//For the R and S data
//		int [][] bounds = {{191,2444,118},{196,2444,119},{201,2444,119},{207,2440,117}};
		
		
//		int[][] bounds = {{177,2443,117},{183,2443,116},{186,2442,118},{188,2443,117},{190,2442,114},{191,2441,116},{192,2442,116},{193,2442,115},{194,2442,114},{194,2443,116},{195,2442,117},{195,2443,118},{196,2442,119},{197,2442,115},{197,2443,118},{198,2443,114},{199,2442,114},{199,2443,116},{200,2441,120},{200,2443,115},{201,2441,117},{201,2443,115},{202,2442,113},{202,2443,116},{203,2442,115},{203,2443,117},{204,2442,117},{204,2443,117},{205,2442,117},{205,2443,117},{206,2442,117},{206,2443,117},{207,2442,117},{207,2443,119},{208,2443,115},{209,2442,116},{209,2444,117},{210,2443,116},{211,2442,117}};
		
		
		//The threshold is 7,partition is 40
//		int[][] bounds = {{1777,2114,112},{1836,2114,112},{1864,2114,112},{1887,2114,112},{1900,2114,112},{1910,2114,112},{1922,2114,112},{1931,2114,112},{1942,2114,112},{1948,2114,112},{1954,2114,112},{1960,2114,112},{1966,2114,112},{1972,2114,112},{1979,2114,112},{1986,2114,112},{1992,2114,112},{1997,2114,112},{2002,2114,112},{2007,2114,112},{2011,2114,112},{2016,2114,112},{2022,2114,112},{2027,2114,112},{2034,2114,112},{2039,2114,112},{2045,2114,112},{2050,2114,112},{2055,2114,112},{2059,2114,112},{2063,2114,112},{2068,2114,112},{2072,2114,112},{2078,2114,112},{2085,2114,112},{2092,2114,112},{2099,2114,112},{2106,2114,112},{2113,2114,112}};

//		int[][] bounds = {{1800,2114,112},{1860,2114,112},{1893,2114,112},{1908,2114,112},{1925,2114,112},{1938,2114,112},{1948,2114,112},{1955,2114,112},{1963,2114,112},{1972,2114,112},{1981,2114,112},{1990,2114,112},{1996,2114,112},{2003,2114,112},{2009,2114,112},{2015,2114,112},{2022,2114,112},{2029,2114,112},{2037,2114,112},{2044,2114,112},{2051,2114,112},{2057,2114,112},{2062,2114,112},{2069,2114,112},{2075,2114,112},{2084,2114,112},{2093,2114,112},{2102,2114,112},{2110,2114,112}};
		
		//The Threshold is 7 and partition is 25
//		int[][] bounds ={{1784,2114,112},{1861,2114,112},{1897,2114,112},{1915,2114,112},{1932,2114,112},{1947,2114,112},{1956,2114,112},{1966,2114,112},{1977,2114,112},{1988,2114,112},{1996,2114,112},{2004,2114,112},{2011,2114,112},{2019,2114,112},{2027,2114,112},{2037,2114,112},{2047,2114,112},{2054,2114,112},{2061,2114,112},{2068,2114,112},{2075,2114,112},{2086,2114,112},{2098,2114,112},{2108,2114,112}};
		
		//The computation and partition is 30
//		int[][] bounds = {{1056,2114,112},{1856,2114,112},{1903,2114,112},{1928,2114,112},{1945,2114,112},{1951,2114,112},{1958,2114,112},{1966,2114,112},{1975,2114,112},{1984,2114,112},{1992,2114,112},{1997,2114,112},{2003,2114,112},{2008,2114,112},{2012,2114,112},{2017,2114,112},{2023,2114,112},{2029,2114,112},{2037,2114,112},{2044,2114,112},{2050,2114,112},{2055,2114,112},{2059,2114,112},{2062,2114,112},{2068,2114,112},{2072,2114,112},{2079,2114,112},{2088,2114,112},{2099,2114,112},{2109,2114,112}};
		
		
		//1
//		int[][] bounds = {{1056,2114,112},{1425,2114,112},{1450,2114,112},{1487,2114,112},{1494,2114,112},{1503,2114,112},{1512,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1829,2114,112},{1885,2114,112},{1915,2114,112},{1941,2114,112},{1955,2114,112},{1968,2114,112},{1984,2114,112},{1996,2114,112},{2007,2114,112},{2019,2114,112},{2033,2114,112},{2047,2114,112},{2055,2114,112},{2064,2114,112},{2075,2114,112},{2090,2114,112},{2106,2114,112},{2122,2114,112}};
		
		
		//2
//		int[][] bounds = {{1056,2114,112},{1494,2114,112},{1503,2114,112},{1512,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1779,2114,112},{1876,2114,112},{1911,2114,112},{1932,2114,112},{1945,2114,112},{1955,2114,112},{1968,2114,112},{1985,2114,112},{1996,2114,112},{2003,2114,112},{2009,2114,112},{2018,2114,112},{2028,2114,112},{2041,2114,112},{2051,2114,112},{2058,2114,112},{2065,2114,112},{2073,2114,112},{2088,2114,112},{2105,2114,112},{2122,2114,112}};
		
		
		//3
//		int[][] bounds = {{1056,2114,112},{1572,2114,112},{1588,2114,112},{1594,2114,112},{1599,2114,112},{1605,2114,112},{1779,2114,112},{1876,2114,112},{1911,2114,112},{1932,2114,112},{1946,2114,112},{1955,2114,112},{1964,2114,112},{1976,2114,112},{1988,2114,112},{1995,2114,112},{2003,2114,112},{2009,2114,112},{2014,2114,112},{2018,2114,112},{2025,2114,112},{2036,2114,112},{2047,2114,112},{2053,2114,112},{2059,2114,112},{2065,2114,112},{2071,2114,112},{2079,2114,112},{2090,2114,112},{2106,2114,112}};
		
		
		//4
//		int[][] bounds = {{1056,2114,112},{1599,2114,112},{1605,2114,112},{1829,2114,112},{1893,2114,112},{1920,2114,112},{1941,2114,112},{1949,2114,112},{1956,2114,112},{1964,2114,112},{1974,2114,112},{1984,2114,112},{1991,2114,112},{1996,2114,112},{2003,2114,112},{2008,2114,112},{2012,2114,112},{2018,2114,112},{2024,2114,112},{2033,2114,112},{2041,2114,112},{2049,2114,112},{2054,2114,112},{2058,2114,112},{2062,2114,112},{2068,2114,112},{2071,2114,112},{2079,2114,112},{2090,2114,112},{2107,2114,112}};
		
		
		//5
//		int[][] bounds = {{1056,2114,112},{1856,2114,112},{1903,2114,112},{1928,2114,112},{1945,2114,112},{1951,2114,112},{1958,2114,112},{1966,2114,112},{1975,2114,112},{1984,2114,112},{1992,2114,112},{1997,2114,112},{2003,2114,112},{2008,2114,112},{2012,2114,112},{2017,2114,112},{2023,2114,112},{2029,2114,112},{2037,2114,112},{2044,2114,112},{2050,2114,112},{2055,2114,112},{2059,2114,112},{2062,2114,112},{2068,2114,112},{2072,2114,112},{2079,2114,112},{2088,2114,112},{2099,2114,112},{2109,2114,112}};
//		int[][] bounds = new int[30][3];
//		
//		
//		
//		for(int i = 0;i < 30;i++){
//			for(int j = 0; j < 3;j++){
//				int m = FlickrSimilarityJoin.bound[i][j];
//				bounds[i][j] = m;
//			}
//		}
//		
		//threshold 14
		//1
//		int[][] bounds = {{528,2114,112},{623,2114,112},{645,2114,112},{652,2114,112},{724,2114,112},{745,2114,112},{746,2114,112},{768,2114,112},{781,2114,112},{793,2114,112},{799,2114,112},{802,2114,112},{914,2114,112},{945,2114,112},{959,2114,112},{971,2114,112},{978,2114,112},{985,2114,112},{993,2114,112},{999,2114,112},{1004,2114,112},{1009,2114,112},{1013,2114,112},{1019,2114,112},{1023,2114,112},{1028,2114,112},{1033,2114,112},{1038,2114,112},{1045,2114,112},{1053,2114,112}};

		//2
//		int[][] bounds = {{528,2114,112},{724,2114,112},{745,2114,112},{746,2114,112},{768,2114,112},{781,2114,112},{793,2114,112},{799,2114,112},{802,2114,112},{914,2114,112},{943,2114,112},{958,2114,112},{970,2114,112},{977,2114,112},{982,2114,112},{988,2114,112},{993,2114,112},{998,2114,112},{1002,2114,112},{1005,2114,112},{1009,2114,112},{1013,2114,112},{1019,2114,112},{1024,2114,112},{1028,2114,112},{1031,2114,112},{1035,2114,112},{1041,2114,112},{1049,2114,112},{1055,2114,112}};


		//3
//		int[][] bounds = {{528,2114,112},{745,2114,112},{746,2114,112},{768,2114,112},{781,2114,112},{793,2114,112},{799,2114,112},{802,2114,112},{914,2114,112},{943,2114,112},{958,2114,112},{970,2114,112},{976,2114,112},{981,2114,112},{988,2114,112},{994,2114,112},{998,2114,112},{1002,2114,112},{1005,2114,112},{1009,2114,112},{1013,2114,112},{1019,2114,112},{1023,2114,112},{1026,2114,112},{1029,2114,112},{1032,2114,112},{1035,2114,112},{1039,2114,112},{1045,2114,112},{1053,2114,112}};

		//4
//		int[][] bounds = {{528,2114,112},{799,2114,112},{802,2114,112},{914,2114,112},{942,2114,112},{956,2114,112},{967,2114,112},{973,2114,112},{977,2114,112},{982,2114,112},{987,2114,112},{993,2114,112},{996,2114,112},{999,2114,112},{1001,2114,112},{1004,2114,112},{1006,2114,112},{1010,2114,112},{1013,2114,112},{1018,2114,112},{1023,2114,112},{1026,2114,112},{1028,2114,112},{1030,2114,112},{1032,2114,112},{1035,2114,112},{1038,2114,112},{1042,2114,112},{1049,2114,112},{1055,2114,112}};


		//5
		int[][] bounds = {{528,2114,112},{914,2114,112},{947,2114,112},{960,2114,112},{971,2114,112},{975,2114,112},{979,2114,112},{983,2114,112},{988,2114,112},{993,2114,112},{996,2114,112},{999,2114,112},{1002,2114,112},{1004,2114,112},{1006,2114,112},{1009,2114,112},{1011,2114,112},{1014,2114,112},{1018,2114,112},{1022,2114,112},{1025,2114,112},{1027,2114,112},{1029,2114,112},{1031,2114,112},{1033,2114,112},{1035,2114,112},{1037,2114,112},{1041,2114,112},{1047,2114,112},{1054,2114,112}};

		
		
		//bri
		//3
//		int[][] bounds = {{13959,2114,112},{13962,2114,112},{13969,2114,112},{13975,2114,112},{13976,2114,112},{13977,2114,112},{14073,2114,112},{14147,2114,112},{14184,2114,112},{14207,2114,112},{14231,2114,112},{14256,2114,112},{14277,2114,112},{14293,2114,112},{14312,2114,112},{14327,2114,112},{14348,2114,112},{14368,2114,112},{14389,2114,112},{14410,2114,112},{14426,2114,112},{14445,2114,112},{14461,2114,112},{14480,2114,112},{14501,2114,112},{14525,2114,112},{14574,2114,112},{14651,2114,112},{14766,2114,112},{14850,2114,112}};
		//4
//		int[][] bounds = {{13959,2114,112},{13976,2114,112},{14038,2114,112},{14095,2114,112},{14164,2114,112},{14186,2114,112},{14207,2114,112},{14228,2114,112},{14246,2114,112},{14266,2114,112},{14284,2114,112},{14298,2114,112},{14312,2114,112},{14327,2114,112},{14346,2114,112},{14364,2114,112},{14382,2114,112},{14399,2114,112},{14418,2114,112},{14435,2114,112},{14451,2114,112},{14467,2114,112},{14482,2114,112},{14501,2114,112},{14525,2114,112},{14566,2114,112},{14627,2114,112},{14721,2114,112},{14795,2114,112},{14864,2114,112}};
		//5
//		int[][] bounds = {{13959,2114,112},{14059,2114,112},{14115,2114,112},{14171,2114,112},{14186,2114,112},{14204,2114,112},{14224,2114,112},{14245,2114,112},{14263,2114,112},{14279,2114,112},{14294,2114,112},{14309,2114,112},{14322,2114,112},{14337,2114,112},{14354,2114,112},{14371,2114,112},{14387,2114,112},{14404,2114,112},{14421,2114,112},{14436,2114,112},{14451,2114,112},{14466,2114,112},{14481,2114,112},{14498,2114,112},{14516,2114,112},{14553,2114,112},{14597,2114,112},{14669,2114,112},{14781,2114,112},{14862,2114,112}};
		
	
		
//		if(tag == FlickrSimilarityUtil.S_tag && timeInterval % 10 == 0){
//			
//			outputKey.set(timeInterval/10 - 1);
//			context.write(outputKey, outputValue);
//			
//		}
//		
//		outputKey.set(timeInterval/10);
//		context.write(outputKey, outputValue);
//		
		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, it should set to three time interval
		
		
		
		/****************************************************************
		 * 
		 * 
		 * The Partition Method
		 * 
		 * 
		 * 
		 ****************************************************************/
		
		if(!outputValue.getTiles().equals("null")){
		
		if(tag == FlickrSimilarityUtil.S_tag){
			
			int pNumber = 0;
			
			if(timeInterval >= bounds[bounds.length-1][0]){
				
				pNumber = bounds.length;
				
			}else{
				
				for(int i = 0; i < bounds.length;i++){
					
					if(timeInterval < bounds[i][0]){
						pNumber = i;
						break;
					}
				}
			}
			
			outputKey.set(pNumber);
			outputValue.setTileNumber((int)timeInterval );
			context.write(outputKey, outputValue);
			
			if(pNumber == 0){
				if(timeInterval- bounds[0][0] == -1){
					outputKey.set(pNumber+1);
					outputValue.setTileNumber((int)timeInterval );
					context.write(outputKey, outputValue);
				}
			}
			
			if(pNumber == bounds.length){
				if(timeInterval- bounds[bounds.length-1][0] == 0){
					outputKey.set(pNumber-1);
					outputValue.setTileNumber((int)timeInterval );
					context.write(outputKey, outputValue);
				}
			}
			
			
			if(pNumber >= 1 && pNumber <= bounds.length-1){
				
				if(timeInterval- bounds[pNumber-1][0] == 0){
					outputKey.set(pNumber-1);
					outputValue.setTileNumber((int)timeInterval );
					context.write(outputKey, outputValue);
				}
				
				
				if(timeInterval- bounds[pNumber][0] == -1){
					outputKey.set(pNumber+1);
					outputValue.setTileNumber((int)timeInterval );
					context.write(outputKey, outputValue);
				}
			}
			
			
		}else{
			
			int pNumber = 0;
			
			if(timeInterval >= bounds[bounds.length-1][0]){
				
				pNumber = bounds.length;
				
			}else{
				
				for(int i = 0; i < bounds.length;i++){
					
					if(timeInterval < bounds[i][0]){
						pNumber = i;
						break;
					}
				}
			}
			
			//for the R set
			outputKey.set(pNumber);
			outputValue.setTileNumber((int)timeInterval);
			context.write(outputKey, outputValue);
		}
		
		}
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The EGO index mapper end at " + System.currentTimeMillis() + "\n" );
	}
}