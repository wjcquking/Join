package org.macau.local.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.macau.local.util.FlickrDataLocalUtil;


/**
 * Author: mb25428
 * Create Date:  3/23/2013
 *
 * Comments: Read the Spatial Temporal Data from the Flickr then create the needed number record
 * JDK version: <JDK 1.7>
 *
 * Modified By: mb25428
 * Modified Date: 4/3/2013
 * Why&What is Modified: and some operation of the System
 * 
 *
 */

public class CreateNewData {

	public static void CreateData(String inputFileName,String outputFileName,int number){
		//read the raw data
		File file = new File(inputFileName);
        BufferedReader reader = null;
        try {
            System.out.println("Read one line");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            FileWriter writer = new FileWriter(outputFileName);
            // One line one time until the null
            while ((tempString = reader.readLine()) != null && line <= number) {
                // The line Number

            	writer.write(tempString + "\n");
                line++;
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
	}
	
	public static long getUnifiedNumber(String flileName){
		
		File file = new File(flileName);
        BufferedReader reader = null;
        Set<String> unifiedSet = new HashSet<String>();
        long line = 1;
        try {
            System.out.println("Read one line");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            while ((tempString = reader.readLine()) != null) {
            	unifiedSet.add(tempString.toString().split(":")[5]);
                line++;
            }
            
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return unifiedSet.size();
	}
	public static void main(String[] args){
//		CreateData(FlickrDataLocalUtil.rRawDataPath,FlickrDataLocalUtil.rDataPath,10000);
//		CreateData(FlickrDataLocalUtil.sRawDataPath,FlickrDataLocalUtil.sDataPath,10000);
		System.out.println(getUnifiedNumber(FlickrDataLocalUtil.rDataPath));
		System.out.println(getUnifiedNumber(FlickrDataLocalUtil.sDataPath));
		
	}
}
