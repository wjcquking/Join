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
	
	public static final int RS_COUNT_COMPUTATION = 597; 
	
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
            
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	long count = Integer.parseInt(values[countPosition]);
            	myCountArray[Integer.parseInt(values[idPosition])] = count;
            	
            	line++;
            	sum += count;
            	
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
		
//		for(int i = 0; i< N;i++){
//			rCountArray[i] = i+1;
//			sCountArray[i] = N-i;
//		}
		
//		rCountArray = new int[]{1,3,5,2,4,6,7,8,9};
//		sCountArray = new int[]{5,2,4,1,3,3,2,4,1};
		
        
        try {
            System.out.println("Read one line");
            
            String tempString = null;
            
            
            
//            String fileAddress = "D:\\360Downloads\\count\\count_7_rs\\part-r-00000" ;
//            
//            File outputFile = new File(rsFile);
            
            if(!rsFile.exists()){
            	rsFile.createNewFile();
            }
            FileWriter writer = new FileWriter(rsFile);
            
           
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            
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
//            		System.out.println(rCountArray[i] + " s " + sCountArray[i-1] + " " + sCountArray[i] + "  " + sCountArray[i+1]);
            		System.out.println(i + " " + result);
            		writer.append(i + " " + result + "\n");
            	}
            }
            
            
            
            
            
//            countArray = new int[]{1,2,3,4,5,6,7};
            
            
            
//            newPartition(0,N-1,k,1);

//            System.out.println("The Result " + minMaxPartition(0,N-1,k));
            
            
//            System.out.println("The Result " + minMaxPartitionNK2WithReplication(0,k));
//            System.out.println("The Result " + getMinMaxValue(minMaxPartitionNK2WithReplicationWithSkyLine(0,k)));
         
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
//            countArray = new int[]{1,2,3,4,5,6,7};
            
            
            
//            newPartition(0,N-1,k,1);

//            System.out.println("The Result " + minMaxPartition(0,N-1,k));
            
//            System.out.println("The Result " + minMaxPartitionNK2WithReplication(0,k));
            
         
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
//        System.out.println("The Result " + minMaxPartitionNK2(0,k));
        
       
        
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
	
	
	
	/*************************************************************************
	 * 
	 * The NK algorithm which can output the boundary point
	 * 
	 * Output Format: The partition ID: the start Point
	 * for example, the first partition  1: 0
	 * 
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return
	 ************************************************************************/
	
//	public static int minMaxPartitionNK2(int startPoint, int partitionNumber){
//		
////		System.out.println(startPoint + "   "+ partitionNumber);
//		int result = -1;
//		
//		if(partitionNumber == 1){
//
//			if(!resultTagArray[startPoint][partitionNumber]){
//				
//				result = 0;
//				for(int j = startPoint;j <= N-1;j++){
//					result+= countArray[j];
//				}
//				resultArray[startPoint][partitionNumber]= result;
//				resultTagArray[startPoint][partitionNumber] = true;
//				
//			}else{
//				
//				result = resultArray[startPoint][partitionNumber];
//				
//			}
////			System.out.println("Result1 " + result);
//			return result;
//		}else{
//		
//			int first = 0;
//			int partitionID = -1;
//			for(int i = startPoint; i < N - partitionNumber+1;i++){
//				
//				if(!resultTagArray[i+1][partitionNumber-1]){
//					int rest = minMaxPartitionNK2(i+1,partitionNumber-1);
//					first += countArray[i];			
//					
//					int temp = max(first,rest);
//					if(result == -1){
//						result = max(first,rest);
//					}
//					
//					if(partitionID == -1){
//						partitionID = startPoint;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//					
//					
//				}else{
//					
//					
////					System.out.println((i+1) + " temp " + (partitionNumber-1));
//					int tempResult = resultArray[i+1][partitionNumber-1];
//					first += countArray[i];	
//					int temp = max(first,tempResult);
//					if(result == -1){
//						result = max(first,tempResult);
//					}
//					
//					//There should be i+1 not startPoint
//					if(partitionID == -1){
//						partitionID = i+1;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//				}
////				System.out.println(partitionID + "    Pab " + result);
//			}
////			System.out.println(startPoint + "  " + partitionNumber);
//			resultArray[startPoint][partitionNumber] = result;
//			resultTagArray[startPoint][partitionNumber] = true;
//			boundPointArray[startPoint][partitionNumber] = partitionID;
////			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
//			
//			if(startPoint == 0 && partitionNumber == k){
//				printResult();
//			}
//			return result;
//		}
//		
//		
//		
//	}
	
	/**********************************************************************************
	 * 
	 * The NK algorithm with replication in the bound point
	 * 
	 * NOTE: The Replication is just occurred in the first Point of every partition
	 * The first partition is no replication, the rest partition copy the last element
	 * of previous partition
	 * 
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return the minimum of all the maximum
	 */
	
//	public static int minMaxPartitionNK2WithReplication(int startPoint, int partitionNumber){
//		
////		System.out.println(startPoint + "   "+ partitionNumber);
//		
//		int replication = 0;
//		
//		int result = -1;
//		
//		if(partitionNumber == 1){
//
//			if(!resultTagArray[startPoint][partitionNumber]){
//				
//				result = 0;
//				
//				for(int j = startPoint == 0 ? startPoint : startPoint-1;j <= N-1;j++){
//					result+= countArray[j];
//				}
//				resultArray[startPoint][partitionNumber]= result;
//				resultTagArray[startPoint][partitionNumber] = true;
//				
//			}else{
//				
//				result = resultArray[startPoint][partitionNumber];
//				
//			}
////			System.out.println(startPoint + "   " + partitionNumber  + " Result1 " + result);
//			return result;
//			
//			
//		}else{
//		
//			int first = 0;
//			if(startPoint != 0){
//				first += countArray[startPoint-1];
//			}
//			int partitionID = -1;
//			for(int i = startPoint; i < N - partitionNumber+1;i++){
//				
//				if(!resultTagArray[i+1][partitionNumber-1]){
//					int rest = minMaxPartitionNK2WithReplication(i+1,partitionNumber-1);
//					first += countArray[i];			
//					
////					System.out.println("first " + first + " rest " + rest);
//					int temp = max(first,rest);
//					if(result == -1){
//						result = max(first,rest);
//					}
//					
//					if(partitionID == -1){
//						partitionID = startPoint;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//					
//					
//				}else{
//					
//					
////					System.out.println((i+1) + " temp " + (partitionNumber-1));
//					int tempResult = resultArray[i+1][partitionNumber-1];
//					first += countArray[i];	
//					int temp = max(first,tempResult);
//					if(result == -1){
//						result = max(first,tempResult);
//					}
//					
//					//There should be i+1 not startPoint
//					if(partitionID == -1){
//						partitionID = i+1;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//				}
//			}
////			System.out.println(startPoint + "  " + partitionNumber);
//			resultArray[startPoint][partitionNumber] = result;
//			resultTagArray[startPoint][partitionNumber] = true;
//			boundPointArray[startPoint][partitionNumber] = partitionID;
////			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
//			
//			if(startPoint == 0 && partitionNumber == k){
//				printResult();
//			}
//			return result;
//		}
//		
//		
//		
//	}

	
	/**********************************************************************************
	 * 
	 * The NK algorithm with replication in the bound point
	 * 
	 * NOTE: The Replication is just occurred in the first Point of every partition
	 * The first partition is no replication, the rest partition copy the last element
	 * of previous partition
	 * 
	 * For each partition, the sum of all the data can not be larger than some value
	 * 
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return the minimum of all the maximum
	 */
	
//	public static int minMaxPartitionNK2WithReplicationWithLimitation(int startPoint, int partitionNumber){
//		
////		System.out.println(startPoint + "   "+ partitionNumber);
//		
//		int replication = 0;
//		
//		int result = -1;
//		
//		if(partitionNumber == 1){
//
//			if(!resultTagArray[startPoint][partitionNumber]){
//				
//				result = 0;
//				
//				for(int j = startPoint == 0 ? startPoint : startPoint-1;j <= N-1;j++){
//					result+= countArray[j];
//				}
//				resultArray[startPoint][partitionNumber]= result;
//				resultTagArray[startPoint][partitionNumber] = true;
//				
//			}else{
//				
//				result = resultArray[startPoint][partitionNumber];
//				
//			}
////			System.out.println(startPoint + "   " + partitionNumber  + " Result1 " + result);
//			return result;
//			
//			
//		}else{
//		
//			int first = 0;
//			if(startPoint != 0){
//				first += countArray[startPoint-1];
//			}
//			int partitionID = -1;
//			for(int i = startPoint; i < N - partitionNumber+1;i++){
//				
//				if(!resultTagArray[i+1][partitionNumber-1]){
//					int rest = minMaxPartitionNK2WithReplication(i+1,partitionNumber-1);
//					first += countArray[i];			
//					
////					System.out.println("first " + first + " rest " + rest);
//					int temp = max(first,rest);
//					if(result == -1){
//						result = max(first,rest);
//					}
//					
//					if(partitionID == -1){
//						partitionID = startPoint;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//					
//					
//				}else{
//					
//					
////					System.out.println((i+1) + " temp " + (partitionNumber-1));
//					int tempResult = resultArray[i+1][partitionNumber-1];
//					first += countArray[i];	
//					int temp = max(first,tempResult);
//					if(result == -1){
//						result = max(first,tempResult);
//					}
//					
//					//There should be i+1 not startPoint
//					if(partitionID == -1){
//						partitionID = i+1;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//				}
//			}
////			System.out.println(startPoint + "  " + partitionNumber);
//			resultArray[startPoint][partitionNumber] = result;
//			resultTagArray[startPoint][partitionNumber] = true;
//			boundPointArray[startPoint][partitionNumber] = partitionID;
////			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
//			
//			if(startPoint == 0 && partitionNumber == k){
//				printResult();
//			}
//			return result;
//		}
//		
//		
//		
//	}
	


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
//				if(i != k-1){
//					int nextId = idArray[p.getBoundPoint()[i+1]];
//					dataValue = rSumArray[nextId] - rSumArray[id] + sSumArray[nextId+1] -sSumArray[id-1];
//				}else{
//					int lastId = idArray[idArray.length-1];
//					dataValue = rSumArray[lastId] - rSumArray[id] + sSumArray[lastId+1] - sSumArray[id-1];
//					
//				}
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
	 * Create User: mb25428
	 * Create Date: 2015-02-03
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-05
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return The List of all the not dominated pair(replication, maximum)
	 * 
	 * NOTE: For each bound point, it should copy the last element of S to the next partition and copy the first
	 * element of S of this partition to the previous partition
	 * 
	 * The SkyLine is to find the all the point(the Replication and the Maximum of all partition) that do not dominated by 
	 * other point. So the point must include the the smallest replication and the minimum of the maximum of all the 
	 * partition
	 * 
	 *
	 */
//	public static List<Point> minMaxPartitionNK2WithReplicationWithSkyLine(int startPoint, int partitionNumber){
//		
//		System.out.println(startPoint + "   "+ partitionNumber);
//		
//		
//		int result = -1;
//		
//		if(partitionNumber == 1){
//
//			if(!resultTagArray[startPoint][partitionNumber]){
//				
//				result = 0;
//				
//				for(int j = startPoint;j <= N-1;j++){
//					result+= countArray[j];
//				}
//				
//				resultArray[startPoint][partitionNumber]= result;
//				listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
//				listResultArray[startPoint][partitionNumber].add(new Point(sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],result));
//				
//				
//				resultTagArray[startPoint][partitionNumber] = true;
//				
//			}else{
//				
//				result = resultArray[startPoint][partitionNumber];
//				
//			}
//			List<Point> tempList = new ArrayList<Point>();
////			System.out.println("Point x " + countArray[startPoint-1] + " y " + result);
//			
////			tempList.add(new Point(countArray[startPoint-1],result));
//			tempList.add(new Point(sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],result));
////			System.out.println(startPoint + "   " + partitionNumber  + " Result1 " + result);
//			
//			System.out.println((sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]) + " Y " + result);
//			return tempList; 
////			return listResultArray[startPoint][partitionNumber];
//			
//			
//		}else{
//			listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
//			
//			int first = 0;
//
//			int partitionID = -1;
//			for(int i = startPoint; i < N - partitionNumber+1;i++){
//				
//				if(!resultTagArray[i+1][partitionNumber-1]){
//					List<Point> pointList = minMaxPartitionNK2WithReplicationWithSkyLine(i+1,partitionNumber-1);
//					
//
//					
//					
//					
//					Point restMaxPoint = getMinMaxPoint(pointList);
//					int rest = restMaxPoint.getY();
//					
//					first += countArray[i];			
//					
////					System.out.println("Rep " + restPoint.getX() + " rest Point " + restPoint.getY()+ " first " + first + " rest " + rest);
//					int temp = max(first,restMaxPoint.getY());
//					if(result == -1){
//						result = max(first,rest);
//					}
//					
//					if(partitionID == -1){
//						partitionID = startPoint;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//					
//					for(Point point : pointList){
//						
////						System.out.println(point.getX()+"  "+startPoint + " " + partitionNumber);
////						tempList.add(new Point(point.getX() + countArray[startPoint-1],result));
//						
//						if(startPoint == 0){
////							System.out.println(startPoint + " " + partitionNumber + " Point x1 " + point.getX() + " y " + max(point.getY(),first) + " size " + pointList.size());
//							dominatedPointInList(new Point(point.getX(),max(point.getY(),first)), listResultArray[startPoint][partitionNumber]);
////							listResultArray[startPoint][partitionNumber].add(new Point(point.getX(),point.getY()));
//						}else{
////							System.out.println(startPoint + " " + partitionNumber + " Point x2 " + (point.getX() + countArray[startPoint-1]) + " y " + temp);
//							dominatedPointInList(new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp), listResultArray[startPoint][partitionNumber]);
////							listResultArray[startPoint][partitionNumber].add(new Point(point.getX() + countArray[startPoint-1],result));
//						}
//					}
//					
//				}else{
//					
//					List<Point> pointList = listResultArray[i+1][partitionNumber-1];
////					System.out.println((i+1) + " temp " + (partitionNumber-1));
////					System.out.println((i+1) + "  " + (partitionNumber-1));
////					System.out.println(pointList);
//					int tempResult = getMinMaxValue(pointList);
//					first += countArray[i];	
//					
//				
//					int temp = max(first,tempResult);
//					if(result == -1){
//						result = max(first,tempResult);
//					}
//					
//					//There should be i+1 not startPoint
//					if(partitionID == -1){
//						partitionID = i+1;
//					}
//					
//					if(result > temp){
//						partitionID = i+1;
//					}
//					result = min(result,temp);
//					
//					for(Point point : pointList){
////						System.out.println(point.getX()+"  "+startPoint + " " + partitionNumber);
////						tempList.add(new Point(point.getX() + countArray[startPoint-1],result));
//						
//						if(startPoint == 0){
////							System.out.println(startPoint + " " + partitionNumber + " Point x3 " + point.getX() + " y " + max(point.getY(),first));
//							dominatedPointInList(new Point(point.getX(),max(point.getY(),first)), listResultArray[startPoint][partitionNumber]);
////							listResultArray[startPoint][partitionNumber].add(new Point(point.getX(),point.getY()));
//						}else{
////							System.out.println(startPoint + " " + partitionNumber + " Point x4 " + (point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]) + " y " + temp);
//							dominatedPointInList(new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp), listResultArray[startPoint][partitionNumber]);
////							listResultArray[startPoint][partitionNumber].add(new Point(point.getX() + countArray[startPoint-1],result));
//						}
//					}
//				}
//			}
////			System.out.println(startPoint + "  " + partitionNumber);
//			resultArray[startPoint][partitionNumber] = result;
//			
//			
//			resultTagArray[startPoint][partitionNumber] = true;
//			boundPointArray[startPoint][partitionNumber] = partitionID;
////			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
//			
//			if(startPoint == 0 && partitionNumber == k){
//				printResult();
//			}
//			return listResultArray[startPoint][partitionNumber];
//		}
//		
//		
//		
//	}

	
	/**********************************************************************************
	 * 
	 * The NK algorithm with replication in the bound point
	 * 
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
	 * The SkyLine is to find the all the point(the Replication and the Maximum of all partition) that do not dominated by 
	 * other point. So the point must include the the smallest replication and the minimum of the maximum of all the 
	 * partition
	 * 
	 * 
	 * Using the array of the point to record the result, including bounding point and each partition value
	 * 
	 *
	 */
//	public static List<Point> minMaxPartitionNK2WithReplicationWithImprovedSkyLine(int startPoint, int partitionNumber){
//		
////		System.out.println(startPoint + "   "+ partitionNumber);
//		
////		if(startPoint == 4 && partitionNumber == 3){
////			System.out.println("fuck");
////		}
//		
//		int result = -1;
//		
//		/*****************************************
//		 * 
//		 * For the partition Number equal to one
//		 * 
//		 * 
//		 ****************************************/
//		if(partitionNumber == 1){
//
//			
//			if(!resultTagArray[startPoint][partitionNumber]){
//				
//				result = 0;
//				
//				for(int j = startPoint;j <= N-1;j++){
//					result+= countArray[j];
//				}
//				
//				resultArray[startPoint][partitionNumber]= result;
//				
//				
//				listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
//				
//				
//				Point tempPoint = new Point(sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],result);
//				tempPoint.getBoundPoint()[k-1]= startPoint;
//				tempPoint.getPartitionValue()[k-1]= result;
//				
//				
//				listResultArray[startPoint][partitionNumber].add(tempPoint);
//				
//				resultTagArray[startPoint][partitionNumber] = true;
//				
//			}
//			
//			
//			return listResultArray[startPoint][partitionNumber]; 
//			
//			
//		}else{
//			
//			listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
//			
//			long first = 0;
//
//			
//			for(int i = startPoint; i < N - partitionNumber+1;i++){
//				
//				if(!resultTagArray[i+1][partitionNumber-1]){
//					
//					
//					List<Point> pointList = minMaxPartitionNK2WithReplicationWithImprovedSkyLine(i+1,partitionNumber-1);
//					
//					
////					Point restMinMaxPoint = getMinMaxPoint(pointList);
//					
//					first += countArray[i];			
//					
////					int temp = max(first,restMinMaxPoint.getY());
//					
//
//					
//					for(Point point : pointList){
//						
//						
//						Point tempPoint = new Point(point);
//						
//						if(startPoint == 0){
//							
//							tempPoint.setX(point.getX());
//							tempPoint.setY(max(point.getY(),first));
//							
////							Point tempPoint = new Point(point.getX(),max(point.getY(),first));
//							tempPoint.getBoundPoint()[0] = 0;
//							tempPoint.getPartitionValue()[0] = first;
//							
//							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
//							
//						}else{
//							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);;
//							tempPoint.setY(max(point.getY(),first));
//							
////							Point tempPoint = new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp);
//							tempPoint.getBoundPoint()[k-partitionNumber] = startPoint;
//							tempPoint.getPartitionValue()[k-partitionNumber] = first;
//							
//							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
//						}
//					}
//					
//				}else{
//					
//					List<Point> pointList = listResultArray[i+1][partitionNumber-1];
//					
//					//First, find the minimum value of all possible partition solutioni
////					Point restMinMaxPoint = getMinMaxPoint(pointList);
////					int tempResult = getMaxValue(pointList);
//					
//					first += countArray[i];	
//					
//				
////					int temp = max(first,restMinMaxPoint.getY());
//
//					
//
////					result = min(result,temp);
//					
//					for(Point point : pointList){
//						
//						Point tempPoint = new Point(point);
//						
//						if(startPoint == 0){
//							tempPoint.setX(point.getX());
//							tempPoint.setY(max(point.getY(),first));
//							
////							Point tempPoint = new Point(point.getX(),max(point.getY(),first));
//							tempPoint.getBoundPoint()[0] = 0;
//							tempPoint.getPartitionValue()[0] = first;
//							
//							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
//							
//						}else{
//							
//							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);
//							tempPoint.setY(max(point.getY(),first));
//							
////							Point tempPoint = new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp);
//							tempPoint.getBoundPoint()[k-partitionNumber] = startPoint;
//							tempPoint.getPartitionValue()[k-partitionNumber] = first;
//							
//							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
//							
//						}
//					}
//				}
//			}
//			
//			
//			resultTagArray[startPoint][partitionNumber] = true;
//			
//			if(startPoint == 0 && partitionNumber == k){
//				printComputationResult();
//			}
//			return listResultArray[startPoint][partitionNumber];
//		}
//		
//		
//		
//	}

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
		
//		if(startPoint == 4 && partitionNumber == 3){
//			System.out.println("fuck");
//		}
		
		
		long result = -1;
		
		/*****************************************
		 * 
		 * For the partition Number equal to one
		 * 
		 * 
		 ****************************************/
		if(partitionNumber == 1){
			if(tempCount <1500){
				System.out.println(startPoint + "   "+ partitionNumber);
				tempCount++;
			}
			
			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
//				int dataSize = 0;
//				dataSize += sCountArray[idArray[startPoint-1]];
				
				for(int j = startPoint;j <= N-1;j++){
					result+= countArray[j];
//					dataSize += sCountArray[idArray[j]];
//					dataSize += rCountArray[idArray[j]];
				}
				
				
//				
//				if(dataSize > spaceLimitation){
//					
//				}
				
				
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
//						System.out.println(tempDataSize  + " limitation " + spaceLimitation);
						continue;
					}
				}
				
				if(tempCount <1500){
//					System.out.println(startPoint + " fuck "+i + "   "+ partitionNumber + "  " + dataSize);
//					tempCount++;
				}
				
				
				if(dataSize > spaceLimitation){
//					System.out.println("hello " + dataSize);
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
							
//							Point tempPoint = new Point(point.getX(),max(point.getY(),first));
							tempPoint.getBoundPoint()[0] = 0;
							tempPoint.getPartitionValue()[0] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
							
//							if(startPoint == 0 && partitionNumber == 30 && tempPoint.getY() == 64072106){
//								System.out.println("0 30  " + tempPoint.getX() + "  " + tempPoint.getY());
//							}
							
						}else{
							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);;
							tempPoint.setY(max(point.getY(),first));
							
//							Point tempPoint = new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp);
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
							
//							Point tempPoint = new Point(point.getX(),max(point.getY(),first));
							tempPoint.getBoundPoint()[0] = 0;
							tempPoint.getPartitionValue()[0] = first;
							
							dominatedPointInList(tempPoint, listResultArray[startPoint][partitionNumber]);
							
//							if(startPoint == 0 && partitionNumber == 30 && tempPoint.getY() == 64072106){
//								System.out.println("0 30  " + tempPoint.getX() + "  " + tempPoint.getY());
//							}
							
						}else{
							
							tempPoint.setX(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]]);
							tempPoint.setY(max(point.getY(),first));
							
//							Point tempPoint = new Point(point.getX() + sCountArray[idArray[startPoint-1]] + sCountArray[idArray[startPoint]],temp);
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
		
		boolean dominate = false;
		boolean dominated = false;
		boolean noDominate = false;
		
		for(int i = 0; i < list.size();i++){
//			System.out.println(i +" " +list.size());
			if(point.getX() <= list.get(i).getX() && point.getY() <= list.get(i).getY()){
				list.remove(i);
				i--;
//				System.out.println(i + " 1 " + point.getX());
				if(add == false){
					add = true;
//					list.add(i,point);
				}else{
//					i--;
				}
			}else if(point.getX() >= list.get(i).getX() && point.getY() >= list.get(i).getY()){
				exit = true;
//				System.out.println(i + " 2 " + point.getX());
				dominated = true;
				
			}else{
				if(addelse == false){
					addelse = true;
//					list.add(point);
				}else{
					
				}
				
			}
		}
		
		if(dominated == true){
			
		}else if(addelse == true){
			list.add(point);
		}
		
//		System.out.println(list.size());
		if(list.size() == 0){
			list.add(point);
		}
		
		if(list == null){
			list = new ArrayList<Point>();
			list.add(point);
		}
		return result;
	}
	
	public static void main(String[] args){
		
//		try {
////			System.setOut(new PrintStream(new FileOutputStream("D:\\output.txt")));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		System.out.println("find the minimum maximum value");

//		GreedyPartition();
//		LocalOptimalPartition();
		optimalPartition();
		 System.out.println("The Result " + getMinMaxPoint(minMaxPartitionNK2WithReplicationWithImprovedSkyLineWithLimitation(0,k)));
//		printlnPartitionPoint();

		
	}
}
