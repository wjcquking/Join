package org.macau.util;

public class SimilarityUtil {

	public static final double threashold = 0.5;
	
	public static final String recordsInputPath = "hdfs://localhost:9000/user/hadoop/input";
	public static final String tokenOutputPath = "hdfs://localhost:9000/user/hadoop/tokens";
	public static final String ridPairsOutputPath = "hdfs://localhost:9000/user/hadoop/ridpair";
	public static final String finalPairsOutputPath = "hdfs://localhost:9000/user/hadoop/finalpair";
	public static final String recordsGeneratePath = "hdfs://localhost:9000/user/hadoop/records";
	public static final String tokenPhase1Path = "hdfs://localhost:9000/user/hadoop/tokenPhase1";
	
	
	public static final int noRecords = 100;
	
	public static final int cellPerRow = 20;
	
	//this is the data column in the recored
	public static final int[] dataColumns = {3,4};
	
	public static final double distanceThreashold = 5;
	
	/*
	 * we can find the calculation in "Can We Beat the Prefix Filtering?
	 * An Adaptive Framework for Similarity Join and Search "
	 */
	public static int getPrefixLength(int length,double alpha){
		if (length == 0) {
            return 0;
        }
        return length - (int) Math.ceil(alpha * length) + 1;
	}
	
	public static void main(String[] args){
		System.out.println("the prefix");
		System.out.println(SimilarityUtil.getPrefixLength(10, 0.5));
	}
}
