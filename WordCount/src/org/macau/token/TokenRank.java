package org.macau.token;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.macau.util.SimilarityUtil;

public class TokenRank {

	public static HashMap<String, Integer> allTokenMap = new HashMap<String, Integer>();
	
	/**
	 * 
	 * @param path
	 * @throws Exception 
	 * get all the ordered tokens from the output of the phase1
	 */
	public static void loadTokens(String path) throws Exception{
		
		//BufferedReader fis = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		String token = null;
		Path pathq = new Path(path);
		FSDataInputStream fsr = hdfs.open(pathq);
		BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8")); 
		int i = 0;
	    while ((token = bis.readLine()) != null) {
	    	allTokenMap.put(token, i++);
	    	//System.out.println(allTokenMap.get(token));
	    }

	}
	
	/**
	 * 
	 * @param tokens
	 * @throws Exception 
	 * reorder the tokens according to the global order
	 * 
	 */
	public static String tokenRanks(Object[] tokens) throws Exception{
		
		if(allTokenMap.isEmpty()){
			loadTokens(SimilarityUtil.tokenOutputPath+"/part-r-00000");
		}
		
		TreeMap<Integer, String> tokenMap = new TreeMap<Integer, String>();
		
		StringBuffer orderedText = new StringBuffer();
		for(Object token :tokens){
			
			if(allTokenMap.get(token.toString()) != null)
				tokenMap.put(allTokenMap.get(token.toString()),token.toString());
		}
		
		//use the treeMap order the tokens
		Iterator it = tokenMap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry entry =(Map.Entry) it.next();
			if(it.hasNext()){
				orderedText.append(entry.getValue().toString() + ",");
			}else{
				orderedText.append(entry.getValue().toString());
			}
		}
		return orderedText.toString();
	}
	
	/**
	 * 返回一个treeSet
	 */
	public static Collection<Integer> getTokenRanks(Iterable<String> tokens) {
		
		if(allTokenMap.isEmpty()){
			try {
				loadTokens(SimilarityUtil.tokenOutputPath+"/part-r-00000");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		TreeSet<Integer> ranksCol = new TreeSet<Integer>();
		for (String token : tokens) {
			Integer rank = allTokenMap.get(token);
			if (rank != null) {
				ranksCol.add(rank);
			}
		}
		return ranksCol;
	}
	public static void main(String[] args) throws Exception{
		
	}
}
