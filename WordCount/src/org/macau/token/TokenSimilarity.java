package org.macau.token;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TokenSimilarity {

	
	public static double getTokenSimilarity(String iToken,String jToken){
		//System.out.println("iToken:" + iToken + "jToken:" + jToken);
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(",")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(",")));
		
		int i_num = itext.size();
		int j_num = jtext.size();
		//System.out.println(i_num + " " + j_num);
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
	
	/**
	 * 
	 * @param iToken
	 * @param jToken
	 * @return if passing filter,return true
	 */
	public static boolean positionFilter(String iToken,String jToken){
		
		return true;
	}
	public static boolean positionFilter(List<String> itext,List<String> jtext){
		
		return true;
		
	}
	
	
	/**
	 * 
	 * @param iToken
	 * @param jToken
	 * @return if passing filter,return true
	 * iToken size must bigger than jToken size multiply threashold
	 * 
	 */
	public static boolean sizeFilter(String iToken,String jToken){
		return true;
	}
	
	public static boolean sizeFilter(List<String> itext,List<String> jtext){
		if(jtext.size() >= itext.size() && itext.size() >= jtext.size()){
			return true;
		}else{
			return false;
		}
	}
	
	
	public static boolean suffixFilter(String iToken,String jToken){
		return true;
	}
	
	
}
