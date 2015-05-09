package org.macau.stjoin.group.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

import org.macau.stjoin.basic.temporal.TemporalComparator;;


public class TemporalJoinReducer extends
	Reducer<LongWritable, FlickrValue, Text, Text>{
		
		
		private final Text text = new Text();
		
		private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		private final Map<Integer,ArrayList<FlickrValue>> sMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		
		private final List<Long> rCount = new ArrayList<Long>();
		private final List<Long> sCount = new ArrayList<Long>();
		private final List<Long> wCount = new ArrayList<Long>();
		
		private long wCompareCount = 0;
		private long tCompareCount = 0;
		private long sCompareCount = 0;
		private long oCompareCount = 0;

		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("Temporal reducer Start at " + System.currentTimeMillis());
		}
		
		public void reduce(LongWritable key, Iterable<FlickrValue> values,
				Context context) throws IOException, InterruptedException{
			
			
			for(FlickrValue value:values){
				
				FlickrValue fv = new FlickrValue(value);
				
			    if(fv.getTag() == FlickrSimilarityUtil.R_tag){
			    	
			    	if(rMap.containsKey(value.getTileNumber())){
				    	
				    	rMap.get(value.getTileNumber()).add(new FlickrValue(fv));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
				    	
				    	list.add(fv);
				    	
				    	rMap.put(value.getTileNumber(), list);
				    	
				    }
			    }else{
			    	
			    	if(sMap.containsKey(value.getTileNumber())){
				    	
				    	sMap.get(value.getTileNumber()).add(new FlickrValue(fv));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
				    	
				    	list.add(fv);
				    	
				    	sMap.put(value.getTileNumber(), list);
				    }
			    }
			    
			    
			}
			
		
			/*****************************
			 * 
			 * 
			 * Sort the record in the Map to solve the compare time
			 * For example
			 * One record in one time interval, it just need compare record before the same position in the adjacent 
			 * time interval
			 * 
			 * Notice: Now this is no use in the following function but I think it is useful
			 * 
			 ****************************/
			long rSetSize = 0;
			long sSetSize = 0;
			long  wholeSize = 0;
			// Sort the List in the Map
			for(java.util.Iterator<Integer> i = rMap.keySet().iterator();i.hasNext();){
				
				
				TemporalComparator comp = new TemporalComparator();
				int obj = i.next();
				System.out.println("R:" + obj + " " + rMap.get(obj).size());
				Collections.sort(rMap.get(obj),comp);
				rSetSize += rMap.get(obj).size();
				
			}
			
			for(java.util.Iterator<Integer> i = sMap.keySet().iterator();i.hasNext();){
				
				TemporalComparator comp = new TemporalComparator();
				int obj = i.next();
				System.out.println("S:" + obj + "  " + sMap.get(obj).size());
				Collections.sort(sMap.get(obj),comp);
				sSetSize += sMap.get(obj).size();
				
			}
			System.out.println(rSetSize);
			wholeSize = sSetSize + rSetSize;
			rCount.add(rSetSize);
			sCount.add(sSetSize);
			wCount.add(wholeSize);
			

			
			for(java.util.Iterator<Integer> obj = rMap.keySet().iterator();obj.hasNext();){
				
				Integer i = obj.next();
				
				
				if(sMap.containsKey(i)){
					
					
					for(int j = 0;j < rMap.get(i).size();j++){
						
						FlickrValue value1 = rMap.get(i).get(j);
						
						//for the same tail, there is no need for comparing
						
						for(int k = 0; k < sMap.get(i).size();k++){
							FlickrValue value2 = sMap.get(i).get(k);
							tCompareCount++;
							if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
								sCompareCount++;
								if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
									
									oCompareCount++;
//									if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
										
										
										long ridA = value1.getId();
							            long ridB = value2.getId();
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
									
						            
						            
							            text.set(ridA + "%" + ridB);
							            context.write(text, new Text(""));
//									}
								}
							}
						}
						
						
					}
				}
				
				if(sMap.containsKey(i+1)){
					System.out.println("SSSSSS"+ (i+1));
					
					for(int j = 0;j < rMap.get(i).size();j++){
						
						FlickrValue value1 = rMap.get(i).get(j);
						
						//for the same tail, there is no need for comparing
						
						for(int k = 0; k < sMap.get(i+1).size();k++){
							FlickrValue value2 = sMap.get(i+1).get(k);
							tCompareCount++;
							if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
								sCompareCount++;
								if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
									
									oCompareCount++;
//									if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
										
										
										long ridA = value1.getId();
							            long ridB = value2.getId();
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
									
						            
						            
							            text.set(ridA + "%" + ridB);
							            context.write(text, new Text(""));
//									}
								}
							}
						}
						
						
					}
				}


				if(sMap.containsKey(i-1)){
					
					
					for(int j = 0;j < rMap.get(i).size();j++){
						
						FlickrValue value1 = rMap.get(i).get(j);
						
						//for the same tail, there is no need for comparing
						
						for(int k = 0; k < sMap.get(i-1).size();k++){
							FlickrValue value2 = sMap.get(i-1).get(k);
							tCompareCount++;
							if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
								sCompareCount++;
								if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
									
									oCompareCount++;
//									if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
										
										
										long ridA = value1.getId();
							            long ridB = value2.getId();
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
									
						            
						            
							            text.set(ridA + "%" + ridB);
							            context.write(text, new Text(""));
//									}
								}
							}
						}
						
						
					}
				}
			}

			
			rMap.clear();
			sMap.clear();
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("clean up");
			System.out.println("The Reducer End at"+System.currentTimeMillis());
			long rMax = 0;
			long rMin = 1000000;
			long rC =0;
			System.out.println("R data set");
			for(long i : rCount){
//				System.out.println(i);
				if(i > rMax){
					rMax = i;
				}
				if(i < rMin){
					rMin = i;
				}
				rC += i;
			}
			
			System.out.println("r Max " + rMax);
			System.out.println("r Min " + rMin);
			System.out.println("r Count" + rC);
			
			
			long sMax = 0;
			long sMin = 1000000;
			long sC =0;
			System.out.println("S data set");
			
			for(long i : sCount){
//				System.out.println(i);
				if(i > sMax){
					sMax = i;
				}
				if(i < sMin){
					sMin = i;
				}
				sC += i;
			}
			
			System.out.println();
			System.out.println("s Max " + sMax);
			System.out.println("s Min " + sMin);
			System.out.println("s Count" + sC);
			

			long wMax = 0;
			long wMin = 1000000;
			long wC =0;
			for(long i : wCount){
				if(i > wMax){
					wMax = i;
				}
				if(i < wMin){
					wMin = i;
				}
				wC += i;
			}
			
			System.out.println("w Max " + wMax);
			System.out.println("w Min " + wMin);
			System.out.println("w Count" + wC);
			
			
			System.out.println("T compare Count " + tCompareCount);
			System.out.println("S compare Count " + sCompareCount);
			System.out.println("Textual Compare Count " + oCompareCount);
			wCompareCount = tCompareCount+ sCompareCount + oCompareCount;
			System.out.println("The total Count " + wCompareCount);
			
			
		}
	}

