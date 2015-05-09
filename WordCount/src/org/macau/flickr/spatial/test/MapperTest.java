package org.macau.flickr.spatial.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.macau.flickr.spatial.basic.BasicSpatialMapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.spatial.Distance;

public class MapperTest {

	public static void main(String[] args){
		ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
		
		for(int i = 0;i < 2;i++){
			FlickrValue fv = new FlickrValue();
			fv.setId(i);
			records.add(fv);
		}
		
		System.out.println(records);
		for (int i = 0; i < records.size(); i++) {
			FlickrValue rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrValue rec2 = records.get(j);
		    	
//		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		long ridA = rec1.getId();
		            long ridB = rec2.getId();
	//	            if (ridA < ridB) {
	//	                long rid = ridA;
	//	                ridA = ridB;
	//	                ridB = rid;
	//	            }
		            
		            System.out.println(ridA + "%" + ridB);
//		    	}
		    	
		    }
		}
	}
}
