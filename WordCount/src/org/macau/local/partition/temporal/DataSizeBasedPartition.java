package org.macau.local.partition.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DataSizeBasedPartition {

	
	//The input file, The count of R and S together size
	public static final File file = new File("D:\\part-rs-00000");
	
	//The Size of the sequence
	public static final int arrayCount = 778;
	public static int N =arrayCount;
	
	public static int[] countArray = new int[arrayCount];
	public static int[] idArray = new int[arrayCount];
	
	
	public static final int idPosition = 0;
	public static final int countPosition = 1;
	
	
	//The Partition Number
	public static final int k = 30;
	
	//Use two dimension array to record the result
	public static boolean[][] resultTagArray = new boolean[N][k+1];
	
	//record the result of P(i,k)
	public static int[][] resultArray = new int[N][k+1];
	
	
	//record the bound point
	public static int[][] boundPointArray = new int[N][k+1];
	
	
	public static int[] boundIdArray = new int[k-1];
	
	
	
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
	 *************************************************************/
	public static void optimalPartition(){
		BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
           
            int line = 0; 
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	countArray[line] = Integer.parseInt(values[countPosition]);
            	
            	idArray[line] = Integer.parseInt(values[idPosition]);
            	
            	line++;
            	
            }

            System.out.println("The Result " + minMaxPartitionNK2WithReplication(0,k) );
            
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
	public static int max(int value1,int value2){
		return value1 > value2 ? value1 :value2;
	}
	
	
	
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
	
	public static int minMaxPartitionNK2WithReplication(int startPoint, int partitionNumber){
		
		
		int result = -1;
		
		if(partitionNumber == 1){

			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				
				for(int j = startPoint == 0 ? startPoint : startPoint-1;j <= N-1;j++){
					result+= countArray[j];
				}
				
				resultArray[startPoint][partitionNumber]= result;
				resultTagArray[startPoint][partitionNumber] = true;
			}
			
			return resultArray[startPoint][partitionNumber];
			
			
		}else{
		
			int first = 0;
			
			if(startPoint != 0){
				first += countArray[startPoint-1];
			}
			
			int partitionID = -1;
			
			for(int i = startPoint; i < N - partitionNumber+1;i++){
				
				if(!resultTagArray[i+1][partitionNumber-1]){
					int rest = minMaxPartitionNK2WithReplication(i+1,partitionNumber-1);
					first += countArray[i];			
					
//					System.out.println("first " + first + " rest " + rest);
					int temp = max(first,rest);
					if(result == -1){
						result = max(first,rest);
					}
					
					if(partitionID == -1){
						partitionID = startPoint;
					}
					
					if(result > temp){
						partitionID = i+1;
					}
					result = min(result,temp);
					
					
				}else{
					
					
//					System.out.println((i+1) + " temp " + (partitionNumber-1));
					int tempResult = resultArray[i+1][partitionNumber-1];
					first += countArray[i];	
					int temp = max(first,tempResult);
					if(result == -1){
						result = max(first,tempResult);
					}
					
					//There should be i+1 not startPoint
					if(partitionID == -1){
						partitionID = i+1;
					}
					
					if(result > temp){
						partitionID = i+1;
					}
					result = min(result,temp);
				}
			}
//			System.out.println(startPoint + "  " + partitionNumber);
			resultArray[startPoint][partitionNumber] = result;
			resultTagArray[startPoint][partitionNumber] = true;
			boundPointArray[startPoint][partitionNumber] = partitionID;
//			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
			
			if(startPoint == 0 && partitionNumber == k){
				printResult();
			}
			return result;
		}
		
		
		
	}
	
	
	
	/*****************************************************************************************
	 * 
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-01
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-04
	 * 
	 * 
	 * 
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
	 ***************************************************************************************/
	public static void printResult(){
		
		System.out.println(1 + "  Line " + 0 + "  Point " + idArray[0] + " value " + countArray[0]);
		int point = boundPointArray[0][k];
		
		boundIdArray[2-2] = idArray[point];
		
		System.out.println(2 + "  Line " + point + "  Point " + idArray[point] + " value " + countArray[point]);
		
		for(int l = k; l > 2;l--){
			point = boundPointArray[point][l-1];
			boundIdArray[k-l+3-2] = idArray[point];
			System.out.println( (k-l+3) + "  Line " + point+ "  Point " + idArray[point] + " value " + countArray[point]);
		}
	}

	
	/*******************************************************************************
	 * 
	 * 
	 * output the form that is suited for the MapReduce program partition
	 * 
	 * 
	 *****************************************************************************/
	public static void printlnPartitionPoint(){
		
		String result = "{" + idArray[0]+ "," + 2114 +"," + 112+ "},";
		String output = idArray[0]+ ",";
		
		for(int id : boundIdArray){
			result += "{" + id + "," + 2114 +"," + 112+ "},";
			output += id + ",";
			
		}
		System.out.println(result);
		System.out.println(output);
	}
	
	public static void main(String[] args){
		
		System.out.println("find the minimum maximum value");
		long start = System.currentTimeMillis();
		optimalPartition();
		printlnPartitionPoint();
		
		System.out.println((System.currentTimeMillis() -start));
		
	}
}
