package org.macau.spatial;

import java.util.ArrayList;
import java.util.List;

import org.macau.util.SimilarityUtil;


/**
 * 
 * @author hadoop
 * partition the all area to cells
 *
 */

public class CellPartition {
	
	
	public static int getCellId(int x, int y){
		
		return (int)(y/5)*SimilarityUtil.cellPerRow + (int)(x/5)+1;
		
	}
	/**
	 * 
	 * @param cellId
	 * @return the list the record should send to 
	 * for cellId
	 */
	public static List<Integer> getCells(int cellId){
		
		int cellPerRow = SimilarityUtil.cellPerRow;
		List<Integer> list = new ArrayList<Integer>();
		

		if(cellId <= cellPerRow){
			list.add(cellId);
			if(cellId > 1){
				list.add(cellId-1);
			}
		}else{
			list.add(cellId);
			list.add(cellId -cellPerRow);
			if(cellId%cellPerRow != 1){
				list.add(cellId-1);
				list.add(cellId-cellPerRow-1);
			}
			if(cellId%cellPerRow != 0){
				list.add(cellId-cellPerRow+1);
			}
		}
		return list;
		
	}
	
	public static double getDistance(int x1,int y1,int x2,int y2){
		return Math.sqrt((x1-x2)*(x1-x2)+(y1-y2)*(y1-y2));
	}
	
	public static void main(String[] args){
		int x = 62;
		int y = 7;
		int cellId = (int)(y/2.5)*20 + (int)(x/2.5);
		List<Integer> list = getCells(cellId);
		for(int i : list){
			System.out.println(i);
		}
		
	}
}
