package org.macau.flickr.util.spatial;

public class ZOrderValue {

	public static String toFullBinaryString(int num){
		char[] chs = new char[Integer.SIZE];
		for(int i = 0; i < Integer.SIZE;i++){
			chs[Integer.SIZE-1-i] = (char)(((num >> i) & 1) + '0');
		}
		return new String(chs);
	}
	
	/**
	 * Find the z curve order (=vertex index) for the given grid cell 
	 * coordinates.
	 * @param x cell column (from 0)
	 * @param y cell row (from 0)
	 * @param r resolution of z curve (grid will have Math.pow(2,r) 
	 * rows and cols)
	 * @return z order value
	 */
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
	
	public static void main(String args){
		System.out.println("fuck the z order");
	}
}
