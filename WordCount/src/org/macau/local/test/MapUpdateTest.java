package org.macau.local.test;

import java.util.HashMap;
import java.util.Map;

public class MapUpdateTest {

	public static void main(String[] args){
		Map<Integer,Integer> textualAccount = new HashMap<Integer,Integer>();
		textualAccount.put(1, 1);
		textualAccount.put(1, textualAccount.get(1)+1);
		System.out.println(textualAccount.size());
		System.out.println(textualAccount.get(1));
		
		
	}
}
