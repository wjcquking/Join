package org.macau.token;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.macau.util.SimilarityUtil;


/**
 * 
 * @author hadoop
 * load the pair record which is the output of the Phase2
 */
public class LoadPairs {

	public static HashMap<String, HashSet<String>> pairMap = new HashMap<String, HashSet<String>>();
	
	public static void loadParisRecord(String path) throws Exception{
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		String token = null;
		Path pathq = new Path(path);
		FSDataInputStream fsr = hdfs.open(pathq);
		BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8"));
		
	    while ((token = bis.readLine()) != null) {
	    	
			String[] record = token.toString().split("\\s+");
			
			String[] idPairs = record[0].split("%");
			
			for(int i = 0;i < 2;i++){
				HashSet<String> set = new HashSet<String>();
				if(pairMap.get(idPairs[i]) == null){
					if(i == 0)
						set.add(idPairs[1]+"%"+record[1]);
					else
						set.add(idPairs[0]+"%"+record[1]);
					pairMap.put(idPairs[i], set);
					//pairMap.put(id,)
				}else{
					set = pairMap.get(idPairs[i]);
					if(i == 0)
						set.add(idPairs[1]+"%"+record[1]);
					else
						set.add(idPairs[0]+"%"+record[1]);
					pairMap.put(idPairs[i], set); 
				}
			}
	    	
	    }

	}
	public static Set<String> getPairSet(String id) throws Exception{
		
		if(pairMap.isEmpty()){
			loadParisRecord(SimilarityUtil.ridPairsOutputPath+"/part-r-00000");
		}
		
		return  pairMap.get(id);
	}
	
	
	public static void main(String[] args) throws Exception{
		
//		Set pairSet = getPairSet("-214980463");
//		Iterator iterator = pairSet.iterator();
//		while (iterator.hasNext()){
//		   String pair = (String) iterator.next();
//		   System.out.println("123"+pair);
//		}
//		String a = "1,8-home,school#0.6666666666666666";
//		String R1 = a.split("#")[0];
//		String sim = a.split("#")[1];
//		String R2 = a.split("#")[0];
//		System.out.println(R1+ "," + sim + "," + R2);
		
	}
}
