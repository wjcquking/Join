package org.macau.local.flickrbillion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;

public class ProcessTheFlickrData {
	
	public static void main(String[] args){
		System.out.println("The billion flickr data");
		File file = new File("D:\\rawData.txt");
		File infoFile = new File("D:\\info.txt");
		
		BufferedReader reader = null;
		BufferedReader infoReader = null;
		
		try{
			reader = new BufferedReader(new FileReader(file));
			infoReader = new BufferedReader(new FileReader(infoFile));
			String infoString = null;
			String[] infoArray = new String[23];
	
			int j = 0;
			while((infoString = infoReader.readLine()) != null){
				infoArray[j] = infoString;
				j++;
				
			}
			
			
			String tempString = null;
			
			while((tempString = reader.readLine()) != null){
				char [] temp = tempString.toCharArray();
				int count = 0;
				
				
//				for(int i = temp.length - 2; i > 0;i--){
//					if(temp[i] == ' '){
//						count++;
//					}else{
//						if(temp[i+1] == ' '){
//							System.out.print("  " + count);
//						}
//						count=0;
//					}
//					
//				}
//				System.out.println();
//				
//				System.out.println(new Date(1381464628000L));
				
//				for(int i = 1; i < temp.length;i++){
//					if(temp[i] == ' '){
//						count++;
//					}else{
//						if(temp[i-1] == ' '){
//							System.out.print("  " + count);
//						}
//						count=0;
//					}
//					
//				}
//				System.out.println();
				
				String[] flickrData = tempString.split("\t");
				System.out.println(flickrData.length);
				
				for(int i = 0; i < flickrData.length;i++){
					System.out.println(infoArray[i] + "---" + flickrData[i]);
				}

			}
		}catch(Exception e){
			
		}
		
	}

}
