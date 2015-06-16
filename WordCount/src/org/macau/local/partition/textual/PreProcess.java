package org.macau.local.partition.textual;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

/**************************
 * 
 * @author mb25428
 * Because the 0 and 1 is too much 
 * 0	708459
 * 1	377237
 * remove those two value
 * 
 */
public class PreProcess {

public static void main(String[] args){
		
		System.out.println("The Data");
		
//		File file = new File(FlickrDataLocalUtil.sRawDataPath);
//		
//		String outputPath ="D:\\paris.odd.data";
		
		File file = new File("D:\\paris.odd.data");
		
		String outputPath ="E:\\paris.odd.data";
		
        BufferedReader reader = null;
        
        try {
            reader = new BufferedReader(new FileReader(file));
            FileWriter writer = new FileWriter(outputPath);
			
            String tempString = null;
            int line = 1;
            
            // One line one time until the null
            while ((tempString = reader.readLine()) != null) {

                String[] flickrData = tempString.split(":");
                
        		
        		String textual = flickrData[5];
        		String[] text = textual.split(";");
        		
        		String record = flickrData[0] + ":" + flickrData[1] + ":" + flickrData[2] + ":" + flickrData[3] + ":" + flickrData[4];
        		
        		if(text[0].equals("1")){
        			if(text.length > 1){
        				record += ":" + flickrData[5].substring(2);
        				
        			}else{
        				record += ":" + "null";
        			}
        		}else{
        			record += ":" + flickrData[5];
        		}
        		
    			writer.write( record +"\n");;
    				
    			
                line++;
            }
            System.out.println("OK");
           
            writer.close();
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
	}
}
