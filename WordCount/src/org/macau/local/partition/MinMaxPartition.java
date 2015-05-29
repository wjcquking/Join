package org.macau.local.partition;


/******************************************************************************************
 * 
 * @author mb25428
 * Create User : mb25428
 * Create Date : 2015-01-24
 * Description: This Class is to solve the problem of minimizing the maximum value when we 
 * want to partition n sequence to k Partition
 * 
 * 
 * Last Modify User: mb25428
 * Last Modify Date: 2015-01-28
 * 
 * 
 * This file is based on the data size to optimize the data 
 * 
 * The Improved method by adding the computation cost of the data
 * 
 * 
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;





public class MinMaxPartition {
	
	
	
//	public static final File file = new File("D:\\360Downloads\\count\\count_7\\part-r-00000");
	public static final File file = new File("D:\\part-r-00000");
	//The Size of the sequence
	public static final int arrayCount = 458;
	public static int N =458;
	
	public static int[] countArray = new int[arrayCount];
	public static int[] idArray = new int[arrayCount];
	
	
	public static final int idPosition = 0;
	public static final int countPosition = 1;
	
	
	//The Partition Number
	public static final int k = 30;
	
	public static int loop = 0;
	
	public static boolean[][][] tagArray = new boolean[arrayCount][arrayCount][k];
	
	
	//Use two dimension array to record the result
	public static boolean[][] resultTagArray = new boolean[N][k+1];
	//record the result of P(i,k)
	public static int[][] resultArray = new int[N][k+1];
	
	public static List<Point>[][] listResultArray = new ArrayList[N][k+1];
	
	//record the bound point
	public static int[][] boundPointArray = new int[N][k+1];
	
	public static String[] boundArray = new String[k-1];
	
	public static int[] boundIdArray = new int[k-1];
	
	
	public static int resultCount = 0;
	
	
	/**************************************************************
	 * 
	 * The Greedy partition
	 * 
	 * Every partition should be close to the average as much as possible
	 * 
	 *************************************************************/
	public static void GreedyPartition(){
		
        BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_7_partition/part-s-0000" ;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            int[] countArray = new int[arrayCount];
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int count = Integer.parseInt(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
//            	System.out.println(count);
            	line++;
            	sum += count;
            	
            }
            
//            int k = 40;
            int average = sum / k;
            
            //partition number
           
            
            int kNumber =0;
            int realSum = 0;
            int max = average;
            int maxPartitionID = 0;
            int partSum = 0;
            
            for(int i = 0; i < countArray.length;i++){
//            	System.out.println(i + " " + countArray[i]);
            	
            	if(kNumber == 0){
            		int sum1 = Math.abs(partSum-average);
            		int sum2 = Math.abs(partSum+countArray[i] - average);
            		if(sum1 > sum2){
            			partSum += countArray[i];
            		}else{
            			boundArray[kNumber] = stringArray[i];
            			kNumber++;
            			
            			if(partSum < average){
            				max = average;
            			}else{
            				max = partSum;
            			}
            			System.out.println(kNumber + " " + i + "  " + partSum);
            			partSum = 0;
            			i = i-1;
            		}
            	}else{
            		int sum1 = Math.abs(partSum-average);
            		int sum2 = Math.abs(partSum+countArray[i] - average);
            		if(partSum == 0 && partSum+countArray[i] > average){
            			partSum += countArray[i];
            			if(max < partSum){
            				max = partSum;
            				maxPartitionID = kNumber;
            			}
            			
            			System.out.println(kNumber++ + " fuck" + i + "  " + partSum);
            			
            			partSum = 0;
            		}else if(sum1 > sum2){
            			partSum += countArray[i];
            		}else{
            			if(max < partSum){
            				max = partSum;
            				maxPartitionID = kNumber;
            			}

            			kNumber++;
            			System.out.println(kNumber + " " + i + "  " + partSum);
            			realSum += partSum;
            			
            			partSum = 0;
            			i = i-1;
            		}
            		
            	}
            }
            

            System.out.println("The Sum "+sum);
            System.out.println("The Copy Sum " + realSum);
            System.out.println(line);
            System.out.println("Max PartitionID " + maxPartitionID);
            System.out.println("max " + max);
            System.out.println("average "+average);
            System.out.println("K Number " + kNumber);
            reader.close();
            writer.close();
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
	 * The Greedy partition
	 * 
	 * Every partition should be close to the average as much as possible
	 * 
	 * For each partition, it should copy the last element of previous partition
	 * to this partition except the first partition
	 * 
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-01-27
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-01-27
	 * 
	 *************************************************************/
	public static void GreedyPartitionWithReplication(){
		
        BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_7_partition/part-s-0000" ;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            int[] countArray = new int[arrayCount];
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int count = Integer.parseInt(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
//            	System.out.println(count);
            	line++;
            	sum += count;
            	
            }
            
//            int k = 40;
            int average = sum / k;
            
            //partition number
           
            
            int kNumber =0;
            int realSum = 0;
            int max = average;
            int maxPartitionID = 0;
            int partSum = 0;
            
            for(int i = 0; i < countArray.length;i++){
//            	System.out.println(i + " " + countArray[i]);
            	
            	if(kNumber == 0){
            		int sum1 = Math.abs(partSum-average);
            		int sum2 = Math.abs(partSum+countArray[i] - average);
            		if(sum1 > sum2){
            			partSum += countArray[i];
            		}else{
            			boundArray[kNumber] = stringArray[i];
            			kNumber++;
            			
            			if(partSum < average){
            				max = average;
            			}else{
            				max = partSum;
            			}
            			System.out.println(kNumber + " " + i + "  " + partSum);
            			partSum = 0;
            			i = i-2;
            		}
            	}else{
            		int sum1 = Math.abs(partSum-average);
            		int sum2 = Math.abs(partSum+countArray[i] - average);
            		if(partSum == 0 && partSum+countArray[i] > average){
            			partSum += countArray[i];
            			if(max < partSum){
            				max = partSum;
            				maxPartitionID = kNumber;
            			}
            			
            			System.out.println(kNumber++ + " fuck" + i + "  " + partSum);
            			
            			partSum = 0;
            		}else if(sum1 > sum2){
            			partSum += countArray[i];
            		}else{
            			if(max < partSum){
            				max = partSum;
            				maxPartitionID = kNumber;
            			}

            			kNumber++;
            			System.out.println(kNumber + " " + i + "  " + partSum);
            			realSum += partSum;
            			
            			partSum = 0;
            			i = i-2;
            		}
            		
            	}
            }
            

            System.out.println("The Sum "+sum);
            System.out.println("The Copy Sum " + realSum);
            System.out.println(line);
            System.out.println("Max PartitionID " + maxPartitionID);
            System.out.println("max " + max);
            System.out.println("average "+average);
            System.out.println("K Number " + kNumber);
            reader.close();
            writer.close();
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
	
	/********************************************
	 * 
	 * If the data is optimal partitioned, then for the every partition, 
	 * the partition is optimal
	 * For example
	 * For the 1st partition, the difference of the first partition and the average value
	 * of the rest k-1 partition value must be minimize
	 * 
	 * For the second partition, the difference of the second partition and the first partition
	 * and the average value of the rest k-2 partition value must minimize
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-01-15
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-01-27
	 * 
	 * 
	 ********************************************/
	public static void LocalOptimalPartition(){
		
        BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_7_g/part-s-0000" ;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            
            int[] countArray = new int[arrayCount];
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int count = Integer.parseInt(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
            	
//            	System.out.println(count);
            	line++;
            	sum += count;
            	
            }
            
            int average = sum / k;
            
            //partition number
           
            
            int kNumber =0;
            int realSum = 0;
            int max = average;
            int min = average;
            int partSum = 0;
            int restSum = sum;
            
            for(int i = 0; i < countArray.length &&  kNumber < k-1;i++){
            	
            	realSum += countArray[i];
            	
            	int value2 = restSum/(k-kNumber-1);
            	int value3 = (restSum - countArray[i])/(k-kNumber-1);
            	
            	
            	int value4 = (sum - restSum)/(kNumber+1);
            	int value5 = (sum - restSum + countArray[i])/(kNumber+1);
            	
            	int differ1 = Math.abs(partSum - value2);
            	int differ2 = Math.abs(partSum+countArray[i] - value3);
            	
            	if(differ1 > differ2){
            		
            		partSum += countArray[i];
            		restSum = restSum -countArray[i];
            		
            	}else{
        			if(max < partSum){
        				max = partSum;
        			}
        			if(min > partSum){
        				min = partSum;
        			}
//        			kNumber++;
        			
        			boundArray[kNumber] = stringArray[i];
//        			System.out.println("max" + max + "  min " + min);
        			kNumber++;
        			System.out.println(kNumber + " " + i + "  " + partSum + "  " + restSum);
//        			System.out.println(stringArray[i]);
        			partSum = 0;
        			
        			//Because this i is not include in this partition, so the
        			// i should be recalculated 
        			i = i-1;
        		}
            }
            
           

            System.out.println("The Sum "+sum);
            System.out.println("The Copy Sum " + realSum);
            System.out.println(line);
            System.out.println("max " + max);
            System.out.println("average "+average);
            System.out.println("K Number " + kNumber);
//            System.out.println("The difference " + (max - min));
            
            reader.close();
            writer.close();
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
	
	
	/********************************************
	 * 
	 * If the data is optimal partitioned, then for the every partition, 
	 * the partition is optimal
	 * For example
	 * For the 1st partition, the difference of the first partition and the average value
	 * of the rest k-1 partition value must be minimize
	 * 
	 * For the second partition, the difference of the second partition and the first partition
	 * and the average value of the rest k-2 partition value must minimize
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-01-27
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-01-27
	 * 
	 * 
	 ********************************************/
	public static void LocalOptimalPartitionWithReplication(){
		
        BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_7_g/part-s-0000" ;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            
            int[] countArray = new int[arrayCount];
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            int sum = 0;
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int count = Integer.parseInt(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
            	
//            	System.out.println(count);
            	line++;
            	sum += count;
            	
            }
            
            int average = sum / k;
            
            //partition number
           
            
            int kNumber =0;
            int realSum = 0;
            int max = average;
            int min = average;
            int partSum = 0;
            int restSum = sum;
            
            for(int i = 0; i < countArray.length &&  kNumber < k-1;i++){
            	
            	realSum += countArray[i];
            	
            	int value2 = restSum/(k-kNumber-1);
            	int value3 = (restSum - countArray[i])/(k-kNumber-1);
            	
            	
            	int value4 = (sum - restSum)/(kNumber+1);
            	int value5 = (sum - restSum + countArray[i])/(kNumber+1);
            	
            	int differ1 = Math.abs(partSum - value2);
            	int differ2 = Math.abs(partSum+countArray[i] - value3);
            	
            	if(differ1 > differ2){
            		
            		partSum += countArray[i];
            		restSum = restSum -countArray[i];
            		
            	}else{
        			if(max < partSum){
        				max = partSum;
        			}
        			if(min > partSum){
        				min = partSum;
        			}
//        			kNumber++;
        			
        			boundArray[kNumber] = stringArray[i];
//        			System.out.println("max" + max + "  min " + min);
        			kNumber++;
        			System.out.println(kNumber + " " + i + "  " + partSum + "  " + restSum);
//        			System.out.println(stringArray[i]);
        			partSum = 0;
        			
        			//Because this i is not include in this partition, so the
        			// i should be recalculated 
        			i = i-1;
        		}
            }
            
           

            System.out.println("The Sum "+sum);
            System.out.println("The Copy Sum " + realSum);
            System.out.println(line);
            System.out.println("max " + max);
            System.out.println("average "+average);
            System.out.println("K Number " + kNumber);
//            System.out.println("The difference " + (max - min));
            
            reader.close();
            writer.close();
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
	
	public static int[][] result = new int[10000][k];
	
	public static int[] myResult = new int[k];
	
	public static int outputNumber = 0;
	
	
	
	
	/**************************************************************
	 * 
	 * The partition K is based on the partition K-1
	 * 
	 * @param startPoint
	 * @param endPoint
	 * @param partitionNumber
	 */
	public static void partition(int startPoint, int endPoint,int partitionNumber){
		
		
		if(partitionNumber == 1){
			int sum  = 0;
			for(int j = startPoint;j <= endPoint;j++){
				sum+= countArray[j];
				
			}
			tagArray[startPoint][endPoint][1] = true;
			
			for(int m = 0; m < k ; m++){
				if(myResult[m] == -1){
					myResult[m] = endPoint;
					
					if(endPoint == arrayCount-1 && outputNumber < 100){
						for(int o=0;o < k;o++){
							System.out.print(myResult[o] + " fuck ");
						}
						System.out.println();
					}
					break;
				}
				if(myResult[k-1] == arrayCount-1){
					
					for(int o=0;o < k;o++){
						System.out.print(myResult[o] + "  ");
					}
					System.out.println();
					for(int o=0;o < k;o++){
						myResult[o] = -1;
					}
					
					
				}
			}
			
			
//			System.out.println(startPoint + "  " + endPoint + "   "+ sum);
			loop++;
		}else{
			for(int i = startPoint; i <= endPoint - partitionNumber+1;i++){
			
			
				if(!tagArray[startPoint][i][1]){
					partition(startPoint, i,1);
					tagArray[startPoint][i][1] = true;
					loop++;
				}
//				System.out.println(i+1 + "  " + endPoint + "  " + (partitionNumber - 1));
				if(!tagArray[i+1][endPoint][partitionNumber-1]){
					partition(i+1,endPoint,partitionNumber - 1);
					tagArray[i+1][endPoint][partitionNumber-1] = true;
					loop++;
				}
			}
		}
	}
	
	
	/******************************************************************************************
	 * 
	 * 
	 * Has the Partition ID as the tag to show which partition of the data
	 * 
	 * 
	 * @param startPoint
	 * @param endPoint
	 * @param partitionNumber
	 * @param partitionID
	 ****************************************************************************************/
	
	public static void newPartition(int startPoint, int endPoint,int partitionNumber,int partitionID){
		
		if(outputNumber < 100){
			outputNumber++;
			System.out.println(startPoint + "  " + endPoint + "   "+ partitionNumber + "  " + partitionID);
		}
		
		if(partitionNumber == 1){
			int sum  = 0;
			for(int j = startPoint;j <= endPoint;j++){
				sum+= countArray[j];
				
			}
			tagArray[startPoint][endPoint][1] = true;
			
			
			myResult[partitionID -1] = endPoint;
			
			for(int j = partitionID;j < k;j++){
				myResult[j] = -1;
			}
			
			
			if(myResult[k-1] == arrayCount-1){
				
				for(int o=0;o < k;o++){
//					System.out.print(myResult[o] + "  ");
				}
				resultCount++;
//				System.out.println();
				
//			}
			}
			
			loop++;
		}else{
			

			for(int i = startPoint; i <= endPoint - partitionNumber+1;i++){
			
				if(!tagArray[startPoint][i][1]){
					newPartition(startPoint, i,1,partitionID);
					tagArray[startPoint][i][1] = true;
					loop++;
				}
//				System.out.println(i+1 + "  " + endPoint + "  " + (partitionNumber - 1));
				if(!tagArray[i+1][endPoint][partitionNumber-1]){
					newPartition(i+1,endPoint,partitionNumber - 1, k+2-partitionNumber);
					tagArray[i+1][endPoint][partitionNumber-1] = true;
					loop++;
				}
			}
		}
	}
	
	
	public void simpleExample(){
//      countArray = new int[]{1,2,3,4,5,6,7,1,2,3,4,5,6,7};
//      countArray = new int[]{6,5,4,3,2,1};
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
	 *************************************************************/
	public static void optimalPartition(){
		BufferedReader reader = null;
        
        try {
            System.out.println("Read one line");
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            
            
            String fileAddress = "D:/360Downloads/output_7_g/part-s-0000" ;
            
            File outputFile = new File(fileAddress);
            
            if(!outputFile.exists()){
            	outputFile.createNewFile();
            }
            FileWriter writer = new FileWriter(fileAddress);
            
           
            String[] stringArray = new String[arrayCount];
            
            int line = 0; 
            
            while ((tempString = reader.readLine()) != null) {
            	String[] values = tempString.split("\\s+");
            	
            	int count = Integer.parseInt(values[countPosition]);
            	countArray[line] = count;
            	stringArray[line]= tempString;
            	idArray[line] = Integer.parseInt(values[idPosition]);
            	
            	line++;
            	
            }

            System.out.println("The Result " + getMinMaxValue(minMaxPartitionNK2WithReplicationWithSkyLine(0,k)));
            
         
            reader.close();
            writer.close();
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
	
	
	
	
	
	/***************************************************************
	 *
	 * N^2K algorithm for the Dynamic problem
	 * 
	 * 
	 * @param startPoint
	 * @param endPoint
	 * @param partitionNumber
	 * 
	 */
	public static int minMaxPartition(int startPoint, int endPoint,int partitionNumber){
		System.out.println(startPoint + "  " + endPoint + "   "+ partitionNumber);
		int result = -1;
		
		if(partitionNumber == 1){
			result = 0;
			for(int j = startPoint;j <= endPoint;j++){
				result+= countArray[j];
			}
			return result;
		}else{
		
			for(int i = startPoint; i <= endPoint - partitionNumber+1;i++){
				
				int rest = minMaxPartition(i+1,endPoint,partitionNumber-1);
				int first = minMaxPartition(startPoint,i,1);			
				
				int temp = max(first,rest);
				if(result == -1){
					result = max(first,rest);
				}
//				
				result = min(result,temp);
			}
			
			System.out.println("Result " + result);
			return result;
		}
		
		
		
	}
	
	
	/***************************************************************
	 * 
	 * NK algorithm return the final minimum value of all the maximum
	 * 
	 * 
	 * @param startPoint
	 * @param endPoint
	 * @param partitionNumber
	 * 
	 */
	public static int minMaxPartitionNK(int startPoint, int partitionNumber){
		
		System.out.println(startPoint + "   "+ partitionNumber);
		int result = -1;
		
		if(partitionNumber == 1){

			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				for(int j = startPoint;j <= N-1;j++){
					result+= countArray[j];
				}
				resultArray[startPoint][partitionNumber]= result;
				resultTagArray[startPoint][partitionNumber] = true;
				
			}else{
				
				result = resultArray[startPoint][partitionNumber];
				
			}
			System.out.println("Result " + result);
			return result;
		}else{
		
			int first = 0;
			for(int i = startPoint; i < N - partitionNumber+1;i++){
				
				if(!resultTagArray[i+1][partitionNumber-1]){
					int rest = minMaxPartitionNK(i+1,partitionNumber-1);
					first += countArray[i];			
					
					int temp = max(first,rest);
					if(result == -1){
						result = max(first,rest);
					}
					
					result = min(result,temp);
					
				}else{
					
					int tempResult = resultArray[i+1][partitionNumber-1];
					first += countArray[i];	
					int temp = max(first,tempResult);
					if(result == -1){
						result = max(first,tempResult);
					}
					result = min(result,temp);
				}
			}
//			System.out.println(startPoint + "  " + partitionNumber);
			resultArray[startPoint][partitionNumber] = result;
			resultTagArray[startPoint][partitionNumber] = true;
			System.out.println("Result " + result);
			return result;
		}
		
		
		
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
	
	public static int minMaxPartitionNK2(int startPoint, int partitionNumber){
		
//		System.out.println(startPoint + "   "+ partitionNumber);
		int result = -1;
		
		if(partitionNumber == 1){

			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				for(int j = startPoint;j <= N-1;j++){
					result+= countArray[j];
				}
				resultArray[startPoint][partitionNumber]= result;
				resultTagArray[startPoint][partitionNumber] = true;
				
			}else{
				
				result = resultArray[startPoint][partitionNumber];
				
			}
//			System.out.println("Result1 " + result);
			return result;
		}else{
		
			int first = 0;
			int partitionID = -1;
			for(int i = startPoint; i < N - partitionNumber+1;i++){
				
				if(!resultTagArray[i+1][partitionNumber-1]){
					int rest = minMaxPartitionNK2(i+1,partitionNumber-1);
					first += countArray[i];			
					
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
//				System.out.println(partitionID + "    Pab " + result);
			}
//			System.out.println(startPoint + "  " + partitionNumber);
			resultArray[startPoint][partitionNumber] = result;
			resultTagArray[startPoint][partitionNumber] = true;
			boundPointArray[startPoint][partitionNumber] = partitionID;
//			System.out.println(startPoint + " "+ partitionNumber + "  "+ partitionID + "    Result2 " + result);
			
			if(startPoint == 0 && partitionNumber == k){
				System.out.println(1 + "  Point " + 0);
				int point = boundPointArray[0][k];
				System.out.println(2 + "  Point " + point);
				for(int l = k; l > 2;l--){
					
//					System.out.println(point + "  " + (l-1));
					point = boundPointArray[point][l-1];
					
					//The Point is the start Point of every Partition, the start Point of the First Partition
					//is 0
					System.out.println( (k-l+3) + "  Point " + point);
				}
			}
			return result;
		}
		
		
		
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
		
//		System.out.println(startPoint + "   "+ partitionNumber);
		
		int replication = 0;
		
		int result = -1;
		
		if(partitionNumber == 1){

			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				
				for(int j = startPoint == 0 ? startPoint : startPoint-1;j <= N-1;j++){
					result+= countArray[j];
				}
				resultArray[startPoint][partitionNumber]= result;
				resultTagArray[startPoint][partitionNumber] = true;
				
			}else{
				
				result = resultArray[startPoint][partitionNumber];
				
			}
//			System.out.println(startPoint + "   " + partitionNumber  + " Result1 " + result);
			return result;
			
			
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

	
	
	/**********************************************************************************
	 * 
	 * The NK algorithm with replication in the bound point
	 * 
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-01
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-04
	 * 
	 * @param startPoint
	 * @param partitionNumber
	 * @return The List of all the not dominated pair(replication, maximum)
	 * 
	 * NOTE: The Replication is just occurred in the first Point of every partition
	 * The first partition is no replication, the rest partition copy the last element
	 * of previous partition
	 * 
	 * The SkyLine is to find the all the point(the Replication and the Maximum of all partition) that do not dominated by 
	 * other point. So the point must include the the smallest replication and the minimum of the maximum of all the 
	 * partition
	 * 
	 *
	 */
	
	public static List<Point> minMaxPartitionNK2WithReplicationWithSkyLine(int startPoint, int partitionNumber){
		
//		System.out.println(startPoint + "   "+ partitionNumber);
		
		int result = -1;
		
		if(partitionNumber == 1){

			if(!resultTagArray[startPoint][partitionNumber]){
				
				result = 0;
				
				for(int j = startPoint == 0 ? startPoint : startPoint-1;j <= N-1;j++){
					result+= countArray[j];
				}
				resultArray[startPoint][partitionNumber]= result;
				listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
//				dominatedPointInList(new Point(countArray[startPoint-1],result), listResultArray[startPoint][partitionNumber]);
				listResultArray[startPoint][partitionNumber].add(new Point(countArray[startPoint-1],result));
				
				
				resultTagArray[startPoint][partitionNumber] = true;
				
			}else{
				
				result = resultArray[startPoint][partitionNumber];
				
			}
			List<Point> tempList = new ArrayList<Point>();
//			System.out.println("Point x " + countArray[startPoint-1] + " y " + result);
			
			tempList.add(new Point(countArray[startPoint-1],result));
//			System.out.println(startPoint + "   " + partitionNumber  + " Result1 " + result);
			return tempList; 
			
			
		}else{
			List<Point> tempList = new ArrayList<Point>();
			listResultArray[startPoint][partitionNumber] = new ArrayList<Point>();
			
			int first = 0;
			if(startPoint != 0){
				
				first += countArray[startPoint-1];
				
			}
			
			int partitionID = -1;
			for(int i = startPoint; i < N - partitionNumber+1;i++){
				
				if(!resultTagArray[i+1][partitionNumber-1]){
					List<Point> pointList = minMaxPartitionNK2WithReplicationWithSkyLine(i+1,partitionNumber-1);
					

					
					int rest = getMinMaxValue(pointList);
					first += countArray[i];			
					
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
					
					for(Point point : pointList){
						
						
						if(startPoint == 0){
							
							dominatedPointInList(new Point(point.getX(),max(point.getY(),first)), listResultArray[startPoint][partitionNumber]);
							
						}else{
							
							dominatedPointInList(new Point(point.getX() + countArray[startPoint-1],temp), listResultArray[startPoint][partitionNumber]);
							
						}
					}
					
				}else{
					
					List<Point> pointList = listResultArray[i+1][partitionNumber-1];
					int tempResult = getMinMaxValue(pointList);
					first += countArray[i];	
					
				
					int temp = max(first,tempResult);
					if(result == -1){
						result = max(first,tempResult);
					}
					
					if(partitionID == -1){
						partitionID = i+1;
					}
					
					if(result > temp){
						partitionID = i+1;
					}
					result = min(result,temp);
					
					for(Point point : pointList){
						
						if(startPoint == 0){
							
							dominatedPointInList(new Point(point.getX(),max(point.getY(),first)), listResultArray[startPoint][partitionNumber]);
							
						}else{
							
							dominatedPointInList(new Point(point.getX() + countArray[startPoint-1],temp), listResultArray[startPoint][partitionNumber]);
							
						}
					}
				}
			}
			resultArray[startPoint][partitionNumber] = result;
			
			
			resultTagArray[startPoint][partitionNumber] = true;
			boundPointArray[startPoint][partitionNumber] = partitionID;
			
			if(startPoint == 0 && partitionNumber == k){
				printResult();
			}
			return listResultArray[startPoint][partitionNumber];
		}
		
		
		
	}


	/**********************************************************
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-01
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-04
	 * 
	 * @param list
	 * @return the minimum value of the list
	 * 
	 */
	public static int getMinMaxValue(List<Point> list){
		int min = -1;
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
		int i = 0;
		String result = "{" + idArray[0]+ "," + 2114 +"," + 112+ "},";
		for(int id : boundIdArray){
			System.out.println(i++ + " " +id);
			result += "{" + id+ "," + 2114 +"," + 112+ "},";
			
		}
		System.out.println(result);
	}
	
	/**********************************************************
	 * 
	 * Create User: mb25428
	 * Create Date: 2015-02-01
	 * 
	 * Last Modify User: mb25428
	 * Last Modify Date: 2015-02-04
	 * 
	 * @param point
	 * @param list
	 * @return the minimum value of the list
	 * 
	 * 
	 * If the point is dominated by some point in the list, do nothing
	 * If the point dominate some points in the list, remove those points and add the point in the list
	 * If the point do not be dominated or dominate some points, add the point in the list
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
		
		if(list.size() == 0){
			list.add(point);
		}
		return result;
	}
	
	
	public void dominatedTest(){
		
//		listResultArray[0][0] = new ArrayList<Point>();
//		listResultArray[0][0].add(new Point(3,14));
//		listResultArray[0][0].add(new Point(4,12));
//		listResultArray[0][0].add(new Point(5,10));
//		listResultArray[0][0].add(new Point(4,9));
		
//		System.out.println();
//		dominatedPointInList(new Point(4,7), listResultArray[0][3]);
//		for(Point p: listResultArray[0][3]){
//			System.out.println(p.getX() + "  " + p.getY());
//		}
		
	}
	public static void main(String[] args){
		
		System.out.println("find the minimum maximum value");
//		GreedyPartition();
//		LocalOptimalPartition();
		optimalPartition();
		printlnPartitionPoint();
		
		
		System.out.println("The Skyline");
		for(Point p: listResultArray[0][k]){
			System.out.println(p.getX() + "  " + p.getY());
		}

		


	}
}
