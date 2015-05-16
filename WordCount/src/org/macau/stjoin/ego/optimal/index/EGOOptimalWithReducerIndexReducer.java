package org.macau.stjoin.ego.optimal.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValueWithCandidateTags;


public class EGOOptimalWithReducerIndexReducer extends
	Reducer<LongWritable, FlickrValueWithCandidateTags, Text, Text>{
		
		
		private final Text text = new Text();
		
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
		
		private final Map<String, HashMap<Integer, ArrayList<FlickrValueWithCandidateTags>>> rIndex = new HashMap<String,HashMap<Integer,ArrayList<FlickrValueWithCandidateTags>>>();
		private final Map<String, HashMap<Integer, ArrayList<FlickrValueWithCandidateTags>>> sIndex = new HashMap<String,HashMap<Integer,ArrayList<FlickrValueWithCandidateTags>>>();
		
		
	
		
		public void reduce(LongWritable key, Iterable<FlickrValueWithCandidateTags> values,
				Context context) throws IOException, InterruptedException{
			
			
			//there is a index
			// There are three layers, temporal, spatial, textual layer
			for(FlickrValueWithCandidateTags value:values){
//				System.out.println(value.getTag());
				
				FlickrValueWithCandidateTags fv = new FlickrValueWithCandidateTags(value);
				
				String[] candidateKey = fv.getCandidateTags().split("#");
				
				long timeInterval = Long.parseLong(candidateKey[0]);
				int x = Integer.parseInt(candidateKey[1].split(":")[0]);
				int y = Integer.parseInt(candidateKey[1].split(":")[1]);
				
				String[] textual = candidateKey[2].split(",");
				
				
//				String basicKey = timeInterval + "#" + x + ":" + y;
				String basicKey = timeInterval + "#" ;
				
			    if(fv.getTag() == FlickrSimilarityUtil.R_tag){
			    	
			    	
			    	for(long i = timeInterval-1; i <= timeInterval+1;i++){
//			    		for(int j = x-1;j<= x+1;j++){
//			    			for(int k = y-1;k <= y+1;k++){
//			    				String cKey = i + "#" + j + ":" + k;
			    				String cKey = i + "#" ;
			    				
			    				
			    				if(sIndex.containsKey(cKey)){
			    					for(String str : textual){
			    						
			    						int subKey = Integer.parseInt(str);
			    						
			    						if(sIndex.get(cKey).containsKey(subKey)){
				    						for(int m = 0; m < sIndex.get(cKey).get(subKey).size();m++){
				    							
				    							FlickrValueWithCandidateTags value2 = sIndex.get(cKey).get(subKey).get(m);
				    							
				    							
				    							tCompareCount++;
				    							if(FlickrSimilarityUtil.TemporalSimilarity(fv, value2)){
				    								sCompareCount++;
				    								if(FlickrSimilarityUtil.SpatialSimilarity(fv, value2)){
				    									
				    									oCompareCount++;
				    									
				    									if(FlickrSimilarityUtil.TextualSimilarity(fv, value2)){
				    										List<String> itext = new ArrayList<String>(Arrays.asList(fv.getTiles().split(";")));
					    									List<String> jtext = new ArrayList<String>(Arrays.asList(value2.getTiles().split(";")));
					    									
					    									jtext.retainAll(itext);
				    										
				    										long ridA = fv.getId();
				    							            long ridB = value2.getId();
				    							            if (ridA < ridB) {
				    							                long rid = ridA;
				    							                ridA = ridB;
				    							                ridB = rid;
				    							            }
				    									
				    						            
				    							            if(str.equals(jtext.get(0))){
				    							            	text.set(ridA + "%" + ridB);
				    							            	context.write(text, new Text(""));
				    							            }
				    									}
				    								}
//				    							}
//				    						}
			    						}
			    					}
			    				}
			    			}
			    		}
			    	}
			    	
			    	
			    	
			    	if(rIndex.containsKey(basicKey)){
			    		for(String str : textual){
    						
    						int subKey = Integer.parseInt(str);
    						if(rIndex.get(basicKey).containsKey(subKey)){
    							
    							rIndex.get(basicKey).get(subKey).add(fv);
    							
    						}else{
    							ArrayList<FlickrValueWithCandidateTags> list = new ArrayList<FlickrValueWithCandidateTags>();
    					    	
    					    	list.add(fv);
    					    	
    					    	rIndex.get(basicKey).put(subKey, list);
    						}
			    		}
				    	
				    }else{
				    	
				    	HashMap<Integer, ArrayList<FlickrValueWithCandidateTags>> map = new HashMap<Integer,ArrayList<FlickrValueWithCandidateTags>>();
				    	
				    	
				    	for(String str : textual){
    						
    						int subKey = Integer.parseInt(str);
    						
  
							ArrayList<FlickrValueWithCandidateTags> list = new ArrayList<FlickrValueWithCandidateTags>();
    					    	
					    	list.add(fv);
    					    	
					    	map.put(subKey, list);
			    		}
				    	
				    	rIndex.put(basicKey, map);
				    	
				    }
			    	
			    }else{
			    	
			    	for(long i = timeInterval-1; i <= timeInterval+1;i++){
//			    		for(int j = x-1;j<= x+1;j++){
//			    			for(int k = y-1;k <= y+1;k++){
//			    				String cKey = i + "#" + j + ":" + k;
			    				String cKey = i + "#" ;
			    				
			    				
			    				if(rIndex.containsKey(cKey)){
			    					for(String str : textual){
			    						
			    						int subKey = Integer.parseInt(str);
			    						if(rIndex.get(cKey).containsKey(subKey)){
			    						for(int m = 0; m < rIndex.get(cKey).get(subKey).size();m++){
			    							
			    							FlickrValueWithCandidateTags value2 = rIndex.get(cKey).get(subKey).get(m);
			    							
			    							
			    							tCompareCount++;
			    							if(FlickrSimilarityUtil.TemporalSimilarity(fv, value2)){
			    								sCompareCount++;
			    								if(FlickrSimilarityUtil.SpatialSimilarity(fv, value2)){
			    									
			    									oCompareCount++;
			    									if(FlickrSimilarityUtil.TextualSimilarity(fv, value2)){
			    										List<String> itext = new ArrayList<String>(Arrays.asList(fv.getTiles().split(";")));
				    									List<String> jtext = new ArrayList<String>(Arrays.asList(value2.getTiles().split(";")));
				    									
				    									jtext.retainAll(itext);
			    										
			    										long ridA = fv.getId();
			    							            long ridB = value2.getId();
			    							            if (ridA < ridB) {
			    							                long rid = ridA;
			    							                ridA = ridB;
			    							                ridB = rid;
			    							            }
			    									
			    						            
			    						            
			    							            if(str.equals(jtext.get(0))){
			    							            	text.set(ridA + "%" + ridB);
			    							            	context.write(text, new Text(""));
			    							            }
			    									}
			    								}
//			    							}
//			    						}
			    					}
			    					}
			    				}
			    			}
			    		}
			    	}
			    	
			    	
			    	
			    	if(sIndex.containsKey(basicKey)){
			    		for(String str : textual){
    						
    						int subKey = Integer.parseInt(str);
    						if(sIndex.get(basicKey).containsKey(subKey)){
    							
    							sIndex.get(basicKey).get(subKey).add(fv);
    							
    						}else{
    							ArrayList<FlickrValueWithCandidateTags> list = new ArrayList<FlickrValueWithCandidateTags>();
    					    	
    					    	list.add(fv);
    					    	
    					    	sIndex.get(basicKey).put(subKey, list);
    						}
			    		}
				    	
				    }else{
				    	
				    	HashMap<Integer, ArrayList<FlickrValueWithCandidateTags>> map = new HashMap<Integer,ArrayList<FlickrValueWithCandidateTags>>();
				    	
				    	
				    	for(String str : textual){
    						
    						int subKey = Integer.parseInt(str);
    						
  
							ArrayList<FlickrValueWithCandidateTags> list = new ArrayList<FlickrValueWithCandidateTags>();
    					    	
					    	list.add(fv);
    					    	
					    	map.put(subKey, list);
			    		}
				    	
				    	sIndex.put(basicKey, map);
				    	
				    }
			    }
			    
			    
			}
			

			rIndex.clear();
			sIndex.clear();
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

