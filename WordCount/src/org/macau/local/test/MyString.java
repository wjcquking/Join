package org.macau.local.test;

import java.util.ArrayList;
import java.util.List;

class MyString implements Comparable<String> {
    public int compareTo(String str) {        
        return 0;    
    }
    public static void inspect(List<Object> list) {    
        for (Object obj : list) {        
            System.out.println(obj);    
        }    
        list.add(1); //这个操作在当前方法的上下文是合法的。 
    }
    public void test() {    
        List<String> strs = new ArrayList<String>();    
//        inspect(strs); //编译错误 
    } 
    public static void main(String[] args){
    	System.out.println();
//    	MyString a = "abc";
    	String b = "fuck";
//    	a.compareTo(b);
    }
} 
