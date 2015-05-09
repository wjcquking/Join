package org.macau.local.mfeat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.macau.local.util.FlickrData;

public class ReadMFeatData {

public static List<List<Double>>  readFileByLines(String fileName) {
		
        File file = new File(fileName);
        List<List<Double>> records = new ArrayList<List<Double>>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            
            // One line one time until the null
            while ((tempString = reader.readLine()) != null) {
            	line++;
                records.add(ConverToListByLoop(tempString));
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
        return records;
    }


	
	
	public static List<Double> ConvertToListByExpression(String str){
		
		List<Double> list = new ArrayList<Double>();
		
		String matcher = "(?<=[^0-9]*)\\d+(?=[^0-9]*)";
		Pattern p = Pattern.compile(matcher);
		Matcher m = p.matcher(str);
		while(m.find()){
			list.add(Double.parseDouble(m.group()));
		}
		
		return list;
	}
	
	public static List<Double> ConverToListByLoop(String str){
		List<Double> list = new ArrayList<Double>();
		int start = 0;
		int end = 0;
		boolean number = false;
//		System.out.println(str);
		for(int i = 0;i < str.length();i++){
			//32 is the space
			if(str.charAt(i) != 32 && number == false){
				number  = true;
				start = i;
			}
			if(number == true && str.charAt(i) == 32 || i == str.length()-1){
				if(i == str.length()-1){
					end = i+1;
				}else{
					end = i;
				}
				list.add(Double.parseDouble(str.substring(start, end)));
				number = false;
			}
			
		}
		return list;
	}
	
	public static void main(String[] args){
		System.out.println("MFeat");
	
		List<List<Double>> mFeatFou = readFileByLines(MFeatUtil.mFeatFou);
		List<List<Double>> mFeatFac = readFileByLines(MFeatUtil.mFeatFac);
		List<List<Double>> mFeatKar = readFileByLines(MFeatUtil.mFeatKar);
		List<List<Double>> mFeatPix = readFileByLines(MFeatUtil.mFeatPix);
		List<List<Double>> mFeatZer = readFileByLines(MFeatUtil.mFeatZer);
		List<List<Double>> mFeatMor = readFileByLines(MFeatUtil.mFeatMor);
		
		System.out.println(mFeatFou.get(0).size());
		System.out.println(mFeatFac.get(0).size());
		System.out.println(mFeatKar.get(0).size());
		System.out.println(mFeatPix.get(0).size());
		System.out.println(mFeatZer.get(0).size());
		System.out.println(mFeatMor.get(0).size());
	}
}
