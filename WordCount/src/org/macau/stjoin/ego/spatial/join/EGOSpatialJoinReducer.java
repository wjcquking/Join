package org.macau.stjoin.ego.spatial.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.FlickrValueWithKey;


public class EGOSpatialJoinReducer extends
	Reducer<LongWritable, FlickrValueWithKey, Text, Text>{
		
		
		private final Text text = new Text();
		
		private final Map<Long,ArrayList<FlickrValueWithKey>> rMap = new HashMap<Long,ArrayList<FlickrValueWithKey>>();
		private final Map<Long,ArrayList<FlickrValueWithKey>> sMap = new HashMap<Long,ArrayList<FlickrValueWithKey>>();
		
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
		
		public void reduce(LongWritable key, Iterable<FlickrValueWithKey> values,
				Context context) throws IOException, InterruptedException{
			
			
			for(FlickrValueWithKey value:values){
				
				FlickrValueWithKey fv = new FlickrValueWithKey(value);
				
			    if(fv.getTag() == FlickrSimilarityUtil.R_tag){
			    	
			    	if(rMap.containsKey(value.getKey())){
				    	
				    	rMap.get(value.getKey()).add(new FlickrValueWithKey(fv));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValueWithKey> list = new ArrayList<FlickrValueWithKey>();
				    	
				    	list.add(fv);
				    	
				    	rMap.put(value.getKey(), list);
				    	
				    }
			    }else{
			    	
			    	
			    	if(sMap.containsKey(value.getKey())){
				    	
				    	sMap.get(value.getKey()).add(new FlickrValueWithKey(fv));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValueWithKey> list = new ArrayList<FlickrValueWithKey>();
				    	
				    	list.add(fv);
				    	
				    	sMap.put(value.getKey(), list);
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
			for(java.util.Iterator<Long> i = rMap.keySet().iterator();i.hasNext();){
				
				long obj = i.next();
				rSetSize += rMap.get(obj).size();
			}
			
			for(java.util.Iterator<Long> i = sMap.keySet().iterator();i.hasNext();){
				
				long obj = i.next();
				sSetSize += sMap.get(obj).size();
				
			}
			System.out.println(rSetSize);
			wholeSize = sSetSize + rSetSize;
			rCount.add(rSetSize);
			sCount.add(sSetSize);
			wCount.add(wholeSize);
			

			double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
			
			for(java.util.Iterator<Long> obj = rMap.keySet().iterator();obj.hasNext();){
				
				Long i = obj.next();
				
				
				FlickrValue tempValue = rMap.get(i).get(0);
				int x = (int) (tempValue.getLat()/thres);
				int y = (int)(tempValue.getLon()/thres );
				
				System.out.println(i + ": " + FlickrSimilarityUtil.parseToZOrder(x, y));
				
				for(int m = x-1; m <= x+1;m++){
					for(int n = y-1; n <= y+1;n++){
						
						long zValue = FlickrSimilarityUtil.parseToZOrder(m, n);
						System.out.println(zValue);
						
						if(sMap.containsKey(zValue)){
							
							
							for(int j = 0;j < rMap.get(i).size();j++){
								
								FlickrValueWithKey value1 = rMap.get(i).get(j);
								
								//for the same tail, there is no need for comparing
								
								for(int k = 0; k < sMap.get(zValue).size();k++){
									FlickrValueWithKey value2 = sMap.get(zValue).get(k);
									
									for(int o = 0; o < FlickrSimilarityUtil.loop ;o++){
										FlickrSimilarityUtil.TextualSimilarity(value1, value2);
									}
									
									tCompareCount++;
									if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
										sCompareCount++;
										if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
											
											oCompareCount++;
											if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
												
												
												long ridA = value1.getId();
									            long ridB = value2.getId();
									            if (ridA < ridB) {
									                long rid = ridA;
									                ridA = ridB;
									                ridB = rid;
									            }
											
								            
								            
									            text.set(ridA + "%" + ridB);
									            context.write(text, new Text(""));
											}
										}
									}
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

