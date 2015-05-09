package org.macau.flickr.temporal.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.macau.flickr.spatial.test.MapperTest;
import org.macau.flickr.util.FlickrValue;

public class TemporalDataTest {

	public static boolean isTheSameDay(Date d1, Date d2) {  
	    if (d1 != null && d2 != null) {  
	        long time = d1.getTime();  
	        long time2 = d2.getTime();  
	        long MS_OF_ONE_DAY = 8640000;  
	        long l = time/MS_OF_ONE_DAY;  
	        long l2 = time2/MS_OF_ONE_DAY;  
	        return l == l2;  
	    }  
	    return false;  
	}  
	/**
	 * The Data form:
	 * ID;Y;X;timestamp
	 * 973929974000;48.89899;2.380696;1093113743
	 * 
	 */
	public static void main(String[] args){
		String record = "1093113743;48.89899;2.380696;973929974000";
		long id =Long.parseLong(record.split(";")[0]);
		System.out.println(id);
		
		long MS_OF_ONE_DAY = 86400000;  
		
		double y = Double.parseDouble(record.split(";")[1]);
		double x = Double.parseDouble(record.split(";")[2]);
		
		System.out.println(System.currentTimeMillis());
		System.out.println("1093113743");
		Date date = new Date();

		long a = Long.parseLong("1004167107000");
		System.out.println("a"+ a);
		System.out.println(date.getTime());
		Date tempDate = new Date(Long.parseLong("1004693853000"));
		Date tempDate2 = new Date(Long.parseLong("1004167107000"));
		System.out.println(tempDate);
		System.out.println(TemporalDataTest.isTheSameDay(tempDate,tempDate2));
		FlickrValue f = new FlickrValue();
		f.setTimestamp(a);
		
		//System.out.println(tempDate.getDay());
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date dt;
		try {
			dt = sdf.parse("2005-2-19");
			System.out.println(sdf.format(dt));    //输出结果是：2005-2-19
			Date date2=new Date();
			
			SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
			String time=df.format(tempDate2);
			System.out.println(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
}
