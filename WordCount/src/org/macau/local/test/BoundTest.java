package org.macau.local.test;

import org.macau.flickr.util.FlickrSimilarityUtil;

public class BoundTest {

	public static void main(String[] args){
		
		System.out.println("Bound Test");
		int[][] bounds = {{0,2114,112},{6670,2114,112},{6810,2114,112},{6875,2114,112},{6965,2114,112},{7020,2114,112},{7055,2114,112},{7105,2114,112},{7170,2114,112},{7205,2114,112},{7230,2114,112},{7260,2114,112},{7325,2114,112},{7370,2114,112},{7395,2114,112},{7415,2114,112},{7445,2114,112},{7495,2114,112},{7535,2114,112},{7560,2114,112},{7580,2114,112},{7605,2114,112},{7640,2114,112},{7700,2114,112},{7735,2114,112},{7765,2114,112},{7800,2114,112},{7870,2114,112},{7920,2114,112},{7990,2114,112}};
		//R:0, S:1
		int tag = 1;
		
		long timeInterval = 100;
		
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
			
			
			
			if(pNumber == 0){
				pNumber = 1;
//				if(timeInterval- bounds[0][0] == -1){
//					System.out.println(pNumber+1);
//				}
			}
			System.out.println(pNumber);
			
			if(pNumber == bounds.length){
				if(timeInterval- bounds[bounds.length-1][0] == 0){
					System.out.println(pNumber-1);
				}
			}
			
			
			if(pNumber >= 1 && pNumber <= bounds.length-1){
				
				if(timeInterval- bounds[pNumber-1][0] == 0){
					System.out.println(pNumber-1);
				}
				
				
				if(timeInterval- bounds[pNumber][0] == -1){
					System.out.println(pNumber+1);
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
			
			if(pNumber == 0){
				pNumber = 1;
			}
			
			//for the R set
			System.out.println(pNumber);
		}
	}
}
