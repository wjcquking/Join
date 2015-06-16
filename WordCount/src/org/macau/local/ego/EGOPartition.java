package org.macau.local.ego;

public class EGOPartition {

	public static void main(String[] args){
		System.out.println("The EGO partition");
		String rPoint = "0,932,955,971,983,997,1008,1021,1031,1045,2000";
		String sPoint = "0,954,975,988,999,1007,1017,1027,1036,1047,2000";
		
		String[] rList = rPoint.split(",");
		String[] sList = sPoint.split(",");
		
		int[] rArray = new int[rList.length];
		int[] sArray = new int[sList.length];
		
		for(int i  = 0; i < rList.length;i++){
			rArray[i] = Integer.parseInt(rList[i]);
			sArray[i] = Integer.parseInt(sList[i]);
		}
		
		for(int i  = 0;i < rArray.length-1;i++){
			int startPoint = rArray[i];
			int endPoint = rArray[i+1];
			int startBound = 0;
			int endBound = 0;
			
			boolean start = false;
			boolean end = false;
			
			for(int j = 0;j < sArray.length;j++){
				if(startPoint <= sArray[j] && start == false){
					startBound = j;
					start = true;
				}
				
				if(endPoint <= sArray[j] && end == false){
					endBound = j;
					end = true;
				}
			}
			
			System.out.println(rArray[i] + "--" + rArray[i+1] + ":" + startBound + ":" + endBound);
		}
		
		String rPartition = "1:1,2:1,2:2,3:3,4:4,5,6:5,6,7:7,8:8,9:9,10";
		int group = 1;
		for(String str : rPartition.split(":")[group].split(",")){
			System.out.println(str);
		}
		System.out.println(rPartition.split(":")[0].split(",")[0]);
	}
}
