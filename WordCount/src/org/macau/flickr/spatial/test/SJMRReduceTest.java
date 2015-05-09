package org.macau.flickr.spatial.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.util.FlickrValue;

public class SJMRReduceTest {

	
	public static void main(String[] args){
		
		Map<Integer,ArrayList<FlickrValue>> map = new HashMap<Integer,ArrayList<FlickrValue>>();
		ArrayList<FlickrValue> values = new ArrayList<FlickrValue>();
		for(int i = 0;i < 10;i++){
			FlickrValue fv = new FlickrValue();
			fv.setId(i);
			fv.setTag(6287);
			values.add(fv);
		}
		
		int count =0;
//		for(FlickrValue value:values){
//			count++;
//			if(value.getId() == 1){
//				System.out.println("for");
//			}
//			if(map.containsKey(value.getTag())){
//				map.get(value.getTag()).add(value);
//				if(value.getId() == 6){
//					System.out.println("if");
//					System.out.println(map.get(value.getTag()));
//				}
//				if(value.getTag() == 6287){
//					System.out.println("if" + map.get(value.getTag()) + "size" );
//				}
//			}else{
//				if(value.getId() == 1){
//					System.out.println("else");
//				}
//				//System.out.println("2"+ value.getTag());
//				ArrayList<FlickrValue> recordList = new ArrayList<FlickrValue>();
//				recordList.add(value);
//				map.put(new Integer(value.getTag()),recordList);
//				if(value.getTag() == 6287){
//					System.out.println("else" + map.get(value.getTag()));
//				}
//			}
//		}
		for(FlickrValue value:values){
			count++;
			if(value.getId() == 65480044){
				System.out.println("for");
			}
			
			if(map.containsKey(value.getTag())){
				if(value.getTag() == 6287){
					System.out.println(value);
					System.out.println("if before "+ map.get(value.getTag()));
				}
				ArrayList<FlickrValue> recordList = map.get(value.getTag());
				recordList.add(value);
				//map.get(value.getTag()).add(value);
//				text.set(value.toString());
//	            context.write(text, new Text(""));
//	            text.set(map.get(value.getTag()).toString());
//	            context.write(text, new Text(""));
				if(value.getTag() == 6287)
					System.out.println("if after "+ map.get(value.getTag()));
				if(value.getId() == 65480044){
					System.out.println("if");
					System.out.println("if" + map.get(value.getTag()));
				}
				if(value.getTag() == 6287){
					
				}
			}else{
				if(value.getId() == 65480044){
					System.out.println("else");
				}
				//System.out.println("2"+ value.getTag());
				ArrayList<FlickrValue> recordList = new ArrayList<FlickrValue>();
				if(value.getTag() == 6287 &&value.getId() == 65480044)
					System.out.println("else before "+ recordList);
				recordList.add(value);
//				text.set(value.toString());
//	            context.write(text, new Text("else"));
//	            text.set(map.get(value.getTag()).toString());
//	            context.write(text, new Text("else"));
				if(value.getTag() == 6287 && value.getId() == 65480044)
					System.out.println("else after "+ recordList);
				map.put(new Integer(value.getTag()),recordList);
				if(value.getTag() == 6287 && value.getId() == 65480044){
					System.out.println("else" + map.get(value.getTag()));
				}
			}
		}
	}
}
