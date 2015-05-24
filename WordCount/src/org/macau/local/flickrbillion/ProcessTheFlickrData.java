package org.macau.local.flickrbillion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

public class ProcessTheFlickrData {
	
	public static void main(String[] args){
		System.out.println("The billion flickr data");
		File file = new File("E:\\Backup\\Flickr_yahoo labs\\yfcc100m_dataset-1\\yfcc100m_dataset-1");
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
			int count =0;
			Set<String> user = new HashSet<String>();
			
			while((tempString = reader.readLine()) != null){

				String[] flickrData = tempString.split("\t");
				
				int i = 8;
//				for(int i = 0; i < flickrData.length;i++){
					if(flickrData[i].contains(",")){
						user.add(flickrData[1]);
						count++;
						System.out.println(count + "---------" + user.size() + "---- " + ((float)user.size()/count));
						System.out.println(infoArray[i] + "---" +flickrData[1] + "--"+ flickrData[i]);
					}
//				}

			}
		}catch(Exception e){
			
		}
		
	}

}
