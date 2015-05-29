package org.macau.local.partition.improved;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/*********************************************************************
 * 
 * @author mb25428
 * 
 * This partition method is used for the computation Cost
 * Because the real execution time is related to the computation time not the data skew
 * So We Change our objective function to get the computation Cost
 *
 *
 *
 * Create User : mb25428
 * Create Date : 2015-01-28
 * Last Modify User : mb25428
 * Last Modify Date : 2015-01-28
 * 
 */
public class MinMaxComputationPartition {
//	public static final File rsFile = new File("D:\\360Downloads\\count\\count_14\\rs\\part-r-00000");
	public static final File rsFile = new File("D:\\360Downloads\\bound\\rs");
	
	
	//The R File Address
//	public static final File rFile = new File("D:\\360Downloads\\count\\count_14\\r\\part-r-00000");
//	public static final File sFile = new File("D:\\360Downloads\\count\\count_14\\s\\part-r-00000");
	
//	public static final File rFile = new File("D:\\360Downloads\\bound4\\even.data");
//	public static final File sFile = new File("D:\\360Downloads\\bound4\\odd.data");
	
	public static final File rFile = new File("D:\\360Downloads\\count\\count_7_r\\part-r-00000");
	public static final File sFile = new File("D:\\360Downloads\\count\\count_7_s\\part-r-00000");
	
	public static final File pFile = new File("D:\\360Downloads\\count\\count_14\\partition\\part-r-00000");
	
	//The computation of RS result which store in the rsFile
	public static final int RS_COUNT_COMPUTATION = 597; 
	
	
	//the maximum range value in the R and S
	public static final int rangeCount = 2300;
	
	
	//The Size of the sequence
	public static final int arrayCount = RS_COUNT_COMPUTATION;
	public static int N = arrayCount;
	
	
	
	
	//For the partition Point
	public static long[] countArray = new long[arrayCount];
	public static int[] idArray = new int[arrayCount];
	
	
	//For the computation balance
	public static long[] rCountArray = new long[rangeCount];
	public static long[] sCountArray = new long[rangeCount];
	
	
	//For the data statistic.For each one record the sum from the first one to this one
	public static long[] rSumArray = new long[rangeCount];
	public static long[] sSumArray = new long[rangeCount];
	
	
	//The sum Array, for i, the value is the sum from i to N
	public static long[] sumArray = new long[arrayCount];

	
	public static final int idPosition = 0;
	public static final int countPosition = 1;
	
	
	
	//The Partition Number
	public static final int k = 30;
	
	public static int loop = 0;
	
	public static boolean[][][] tagArray = new boolean[arrayCount][arrayCount][k];
	
	
	//Use two dimension array to record the result
	public static boolean[][] resultTagArray = new boolean[N][k+1];
	//record the result of P(i,k)
	public static long[][] resultArray = new long[N][k+1];
	public static List<Point>[][] listResultArray = new ArrayList[N][k+1];
	
	
	//record the bound point
	public static int[][] boundPointArray = new int[N][k+1];
	
	public static String[] boundArray = new String[k-1];
	
	public static int[] boundIdArray = new int[k-1];
	
	
	public static int resultCount = 0;
	
	//For the skyline
	public static double spaceLimitation = 15; 
	public static int[] spaceSumArray = new int[arrayCount];
	
	
	public static void readDataToArray(File myFile, long[] myCountArray){
		BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(myFile));
            String tempString = null;
            
            
            while ((tempString = reader.readLine()) != null) {
            	
            	String[] values = tempString.split("\\s+");
            	
            	long count = Integer.parseInt(values[countPosition]);
            	myCountArray[Integer.parseInt(values[idPosition])] = count;
            	
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
	}
	
	/**************************************************************
	 * 
	 * Dynamic problem programming
	 * 
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-01-12
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-01-28
	 * 
	 * 
	 * First Read the statistic data of Two dataset to the two array
	 * 
	 * 
	 *************************************************************/
	public static void optimalPartition(){
		

		readDataToArray(rFile,rCountArray);
		readDataToArray(sFile,sCountArray);
		
		rSumArray[0] = rCountArray[0];
		sSumArray[0] = rCountArray[0];
		
		for(int i = 1; i < rangeCount;i++){
			rSumArray[i] = rSumArray[i-1] + rCountArray[i];
			sSumArray[i] = sSumArray[i-1] + sCountArray[i-1];
		}
		
        
        try {
            System.out.println("Read one line");
            
            if(!rsFile.exists()){
            	rsFile.createNewFile();
            }
            FileWriter writer = new FileWriter(rsFile);
            
            
            for(int i = 0;i < rangeCount;i++){
            	long result = 0;
            	result += (long)rCountArray[i]*(long)sCountArray[i];
            	if(i> 0){
            		result += (long)rCountArray[i] * (long)sCountArray[i-1];
            	}
            	if(i < rangeCount-1){
            		result += (long)rCountArray[i] * (long)sCountArray[i+1];
            	}
            	
            	if(result != 0){
            		
            		writer.append(i + " " + result + "\n");
            		
            	}
            }
            
         
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	
        }
        
        BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(rsFile));
            String tempString = null;
            
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            int spaceSum = 0;
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	long count = Long.parseLong(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
            	idArray[line] = Integer.parseInt(values[idPosition]);
            	sum += count;
            	spaceSum += rCountArray[Integer.parseInt(values[idPosition])] + sCountArray[Integer.parseInt(values[idPosition])];
            	
            	sumArray[line] = sum;
            	spaceSumArray[line] = spaceSum;
            	
            	line++;
            	
            }
            
            System.out.println(sum);
            System.out.println(spaceSum);
            spaceLimitation = 2 * spaceSum/k;
            System.out.println(spaceLimitation);
            
         
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
       
        
	}
	
	
	
	/*********************************************
	 * 
	 * @param value1
	 * @param value2
	 * @return The minimum value
	 * 
	 **********************************************/
	public static int min(int value1, int value2){
		return value1 < value2 ? value1 : value2;
	}
	
	
	/*********************************************
	 * 
	 * @param value1
	 * @param value2
	 * @return The maximum value
	 * 
	 **********************************************/
	public static long max(long value1,long value2){
		return value1 > value2 ? value1 :value2;
	}
	
	

	/*****************************************************************************************
	 * 
	 * Print the result in the following format
	 * 
	 * Partition ID : Start Point ID : Point Value
	 * 
	 * Partition ID: Start From 1
	 * Start Point ID: Start From 0
	 * Point Value : the Point Value
	 * 
	 * NOTE:The Point is the start Point of every Partition, the start Point of the First 
	 * Partition is 0
	 * 
	 * Using the bound Point Array to record the every Bound point
	 * 
	 ***************************************************************************************/
	public static void printResult(){
		
		System.out.println(1 + "  Line " + 0 + "  Point " + idArray[0] + " value " + countArray[0]);
		int point = boundPointArray[0][k];
		System.out.println(point);
		boundIdArray[2-2] = idArray[point];
		
		System.out.println(2 + "  Line " + point + "  Point " + idArray[point] + " value " + countArray[point] + " S " + sCountArray[idArray[point] ]);
		
		for(int l = k; l > 2;l--){
			point = boundPointArray[point][l-1];
			boundIdArray[k-l+3-2] = idArray[point];
			System.out.println( (k-l+3) + "  Line " + point+ "  Point " + idArray[point] + " value " + countArray[point] + " S " + sCountArray[idArray[point] ]);
		}
	}
	
	
	public static void printComputationResult(){
		
		
		
		
		for(Point p: listResultArray[0][k]){
			System.out.println(p.getX() + " " + p.getY()+ "  " + getMaxValue(p.getPartitionValue()));
//			System.out.println(p.getY());
			long sum = 0;
			String result  = "";
			System.out.println("Line " + " Id "+" PartitionValue " + " CopyVlaue  " + "  DataValue ");
			for(int i = 0;i < k;i++){
				int id = idArray[p.getBoundPoint()[i]];
//				System.out.println(id);
				long copyValue = i == 0 ? 0 : sCountArray[id] + sCountArray[id-1];
				
				int nextId = (i!= k-1) ?idArray[p.getBoundPoint()[i+1]] :idArray[idArray.length-1];
				long dataValue = rSumArray[nextId] - rSumArray[id] + sSumArray[nextId+1] -sSumArray[id-1];

				result += "{" + idArray[p.getBoundPoint()[i]]+ "," + 2114 +"," + 112+ "},";
				System.out.println(p.getBoundPoint()[i] + " " + idArray[p.getBoundPoint()[i]]+"  " + p.getPartitionValue()[i] + " " + copyValue + "  " + dataValue);
				sum += p.getPartitionValue()[i];
			}
			System.out.println(result);
			System.out.println("Sum value is " + sum);
			System.out.println("The Space Limitation " + spaceLimitation);
		}
	}




	/**********************************************************************************
	 * 
	 * The NK algorithm with replication in the bound point
	 * 
	 * 
	 * THE NOTE: There are some problem, the result is not the same with the no limitation
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-06
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-08
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return The List of all the not dominated pair(replication, maximum)
	 * 
	 * NOTE: For each bound point, it should copy the last element of S to the next partition and copy the first
	 * element of S of this partition to the previous partition
	 * 
	 * For the data set, define a threshold for the each machine space 
	 * 
	 * The SkyLine is to find the all the point(the Replication and the Maximum of all partition) that do not dominated by 
	 * other point. So the point must include the the smallest replication and the minimum of the maximum of all the 
	 * partition
	 * 
	 * 
	 * Using the array of the point to record the result, including bounding point and each partition value
	 * 
	 * 
	 * 
	 *
	 */
	public static int tempCount= 0;
	public static List<Point> minMaxPartitionNK2WithReplicationWithImprovedSkyLineWithLimitation(int startPoint, int partitionNumber){
		
		
		long result = -1;
		
		/*****************************************
		 * 
		 * For the partition Number equal to one
		 * 
		 * 
		 ****************************************/
		if(partitionNumber == 1){
			
			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				
				for(int j = startPoint;j <= N-1;j++){
					result+= countArray[j];
				}
				
				
				resultArray[startPoint][partitionNumber]= result;
				
				
				listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
				
				
				Point tempPoint = new Point(sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],result);
				tempPoint.getBoundPoint()[k-1]= startPoint;
				tempPoint.getPartitionValue()[k-1]= result;
				
				
				listResultArray[startPoint][partitionNumber].add(tempPoint);
				
				resultTagArray[startPoint][partitionNumber] = true;
				
			}
			
			
			return listResultArray[startPoint][partitionNumber]; 
			
			
		}else{
			
			listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
			
			long first = 0;

			
			long dataSize = 0;
			
			for(int i = startPoint; i < N - partitionNumber+1;i++){
				
				
				if(i== 0){
					dataSize += sCountArray[idArray[i]];
				}
				
				if(dataSize == 0 && i !=0 ){
					dataSize += sCountArray[idArray[i-1]];
					dataSize += sCountArray[idArray[i]];
				}
				
//				first += countArray[i];			
				dataSize += sCountArray[idArray[i+1]];
				dataSize += rCountArray[idArray[i]];
				
				long tempDataSize = 0;
				if(partitionNumber-1 == 1){
					tempDataSize = spaceSumArray[N-1] - spaceSumArray[i] + sCountArray[idArray[i]];
					if(tempDataSize > spaceLimitation){
						continue;
					}
				}
				
				
				if(dataSize > spaceLimitation){
					
					break;
					
				}
				
				
				if(!resultTagArray[i+1][partitionNumber-1]){
					
					
					
					List<Point> pointList = minMaxPartitionNK2WithReplicationWithImprovedSkyLineWithLimitation(i+1,partitionNumber-1);
					
					

					first += countArray[i];			


					
					for(Point point : pointList){
						
						
						Point tempPoint = new Point(point);
						
						if(startPoint == 0){
							
							tempPoint.setX(point.getX());
							tempPoint.setY(max(point.getY(),first));
							
							tempPoint.getBoundPoint()[0] = 0;
							tempPoint.getPartitionValue()[0] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
							
						}else{
							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);;
							tempPoint.setY(max(point.getY(),first));
							
							tempPoint.getBoundPoint()[k-partitionNumber] = startPoint;
							tempPoint.getPartitionValue()[k-partitionNumber] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
						}
					}
					
				}else{
					
					List<Point> pointList = listResultArray[i+1][partitionNumber-1];

					first += countArray[i];			

					
					for(Point point : pointList){
						
						Point tempPoint = new Point(point);
						
						if(startPoint == 0){
							tempPoint.setX(point.getX());
							tempPoint.setY(max(point.getY(),first));
							
							tempPoint.getBoundPoint()[0] = 0;
							tempPoint.getPartitionValue()[0] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
							
							
						}else{
							
							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);
							tempPoint.setY(max(point.getY(),first));
							
							tempPoint.getBoundPoint()[k-partitionNumber] = startPoint;
							tempPoint.getPartitionValue()[k-partitionNumber] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
							
						}
					}
				}
			}
			
			
			resultTagArray[startPoint][partitionNumber] = true;
			
			if(startPoint == 0 && partitionNumber == k){
				printComputationResult();
			}
			return listResultArray[startPoint][partitionNumber];
		}
		
		
		
	}
	public static long getMaxValue(long[] array){
		
		long max = -1;
		for(long i : array){
			if(max == -1){
				max = i;
			}
			if(max < i){
				max = i;
			}
		}
		return max;
	}
	
	
	public static Point getMaxPoint(List<Point> list){
		
		long max = -1;
		Point result = new Point();
		for(Point point : list){
			if(max == -1){
				max = point.getY();
				result.setX(point.getX());
				result.setY(point.getY());
			}
			if(max < point.getY()){
				max = point.getY();
				result.setX(point.getX());
				result.setY(point.getY());
			}
		}
		
		return result;
	}
	
	public static long getMinMaxValue(List<Point> list){
		long min = -1;
		for(Point point : list){
			if(min == -1){
				min = point.getY();
			}
			if(min > point.getY()){
				min = point.getY();
			}
		}
		return min;
	}
	
	/***************************************************************
	 * 
	 * @param list
	 * @return the minimum value of all the point in the list
	 * 
	 **************************************************************/
	
	public static Point getMinMaxPoint(List<Point> list){
		long min = -1;
		Point result = new Point();
		for(Point point : list){
			if(min == -1){
				min = point.getY();
				result.setX(point.getX());
				result.setY(point.getY());
			}
			if(min > point.getY()){
				min = point.getY();
				result.setX(point.getX());
				result.setY(point.getY());
			}
		}
		
		return result;
	}
	/*******************************************************************************
	 * 
	 * 
	 * output the form that is suited for the MapReduce program partition
	 * 
	 * 
	 *****************************************************************************/
	public static void printlnPartitionPoint(){
		int i = 0;
		String result = "";
		for(int id : boundIdArray){
			System.out.println(i++ + " " +id);
			result += "{" + id+ "," + 2114 +"," + 112+ "},";
			
		}
		System.out.println(result);
	}
	/**************************************************************
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-12
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-25
	 * 
	 * @param point, to judge the dominate point
	 * @param list, the list contain not dominated point
	 * @return
	 * If the point dominate some points, then remove the points
	 * If the point is dominated by one point in the list, do nothing
	 * If the point is not dominated by any point in the list, insert the point to the list
	 * 
	 * Note: Using the brute force to find the the the point dominate or dominated
	 * We can use sort algorithm to sort the data, then we can only compare the  part of data
	 * then the time complexity may from the O(n) to O(logn)
	 * 
	 * 
	 *************************************************************/
	/**
	 * 

	 */
	public static boolean dominatedPointInList(Point point, List<Point> list){
		boolean result = false;
		
		boolean exit = false;
		boolean add = false;
		boolean addelse = false;
		
//		boolean dominate = false;
		boolean dominated = false;
//		boolean noDominate = false;
		
		for(int i = 0; i < list.size();i++){
			if(point.getX() <= list.get(i).getX() && point.getY() <= list.get(i).getY()){
				list.remove(i);
				i--;
				if(add == false){
					add = true;
				}else{
				}
			}else if(point.getX() >= list.get(i).getX() && point.getY() >= list.get(i).getY()){
				exit = true;
				dominated = true;
				
			}else{
				if(addelse == false){
					
					addelse = true;
					
				}else{
					
				}
				
			}
		}
		
		if(dominated == true){
			
		}else if(addelse == true){
			list.add(point);
		}
		
		if(list.size() == 0){
			list.add(point);
		}
		
		if(list == null){
			list = new ArrayList<Point>();
			list.add(point);
		}
		return result;
	}
	
	
	/***********************************************
	 * 
	 *  Write the output of console to the file
	 *  
	 ***********************************************/
	public static void writeConsoleToFile(){
		
		try {
			
			System.setOut(new PrintStream(new FileOutputStream("D:\\output.txt")));
			
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
			
		}
		
	}
	public static void main(String[] args){
		
//		writeConsoleToFile();
		System.out.println("find the minimum maximum value");

		optimalPartition();
		 System.out.println("The Result " + getMinMaxPoint(minMaxPartitionNK2WithReplicationWithImprovedSkyLineWithLimitation(0,k)));

		
	}
}
