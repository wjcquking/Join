package org.macau.local.test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RandomTest {

	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	public static void main(String[] args){
		Random random = new Random();
		System.out.println(random.nextInt(100000));
		System.out.println(random.nextDouble());
		System.out.println(convertDateToString(convertLongToDate(1258276450000L)));
	}
}
