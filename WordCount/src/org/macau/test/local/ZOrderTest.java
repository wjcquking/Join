package org.macau.test.local;

/**
 * 
 * @author hadoop
 * This class is just for test for the z order value
 */
public class ZOrderTest {

	public static String toFullBinaryString(int num){
		char[] chs = new char[Integer.SIZE];
		for(int i = 0; i < Integer.SIZE;i++){
			chs[Integer.SIZE-1-i] = (char)(((num >> i) & 1) + '0');
		}
		return new String(chs);
	}
	
	public static int parseToZOrder(int x, int y){
		
		String xString = toFullBinaryString(x);
		String yString = toFullBinaryString(y);
		String z = "";
		char[] xChar = xString.toCharArray();
		char[] yChar = yString.toCharArray();
		for(int i = 0; i < xChar.length;i++){
			z += yChar[i];
			z += xChar[i];
			
		}
		return Integer.parseInt(z,2);
		
	}
	public static void main(String[] args){
//		System.out.println(10);
//		String i = Integer.toBinaryString(10 >> 1);
//		System.out.println(i);
//		System.out.println(Integer.parseInt(i,2));
		//System.out.println(toFullBinaryString(8));
		System.out.println(parseToZOrder(1,3));
	}
}
