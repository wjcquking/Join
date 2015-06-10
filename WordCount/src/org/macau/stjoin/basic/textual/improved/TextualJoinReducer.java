package org.macau.stjoin.basic.textual.improved;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValueWithMultiFeature;
import org.macau.stjoin.basic.temporal.TemporalComparator;

public class TextualJoinReducer extends
Reducer<Text, FlickrValueWithMultiFeature, Text, Text>{
	
	
	private final Text text = new Text();
	
	private final Map<String,ArrayList<FlickrValueWithMultiFeature>> rMap = new HashMap<String,ArrayList<FlickrValueWithMultiFeature>>();
	private final Map<String,ArrayList<FlickrValueWithMultiFeature>> sMap = new HashMap<String,ArrayList<FlickrValueWithMultiFeature>>();
	
	private final List<Long> rCount = new ArrayList<Long>();
	private final List<Long> sCount = new ArrayList<Long>();
	private final List<Long> wCount = new ArrayList<Long>();
	
	private long wCompareCount = 0;
	private long tCompareCount = 0;
	private long sCompareCount = 0;
	private long oCompareCount = 0;

	
	public void reduce(Text key, Iterable<FlickrValueWithMultiFeature> values,
			Context context) throws IOException, InterruptedException{
		
		
		for(FlickrValueWithMultiFeature value:values){
			
			FlickrValueWithMultiFeature fv = new FlickrValueWithMultiFeature(value);
			fv.setOthers("A");
			
		    if(value.getTag() == FlickrSimilarityUtil.R_tag){
		    	
		    	if(rMap.containsKey(value.getTileTag())){
			    	
			    	rMap.get(value.getTileTag()).add(new FlickrValueWithMultiFeature(fv));
			    	
			    }else{
			    	
			    	ArrayList<FlickrValueWithMultiFeature> list = new ArrayList<FlickrValueWithMultiFeature>();
			    	
			    	list.add(fv);
			    	
			    	rMap.put(value.getTileTag(), list);
			    	
			    }
		    }else{
		    	
		    	if(sMap.containsKey(value.getTileTag())){
			    	
			    	sMap.get(value.getTileTag()).add(new FlickrValueWithMultiFeature(fv));
			    	
			    }else{
			    	
			    	ArrayList<FlickrValueWithMultiFeature> list = new ArrayList<FlickrValueWithMultiFeature>();
			    	
			    	list.add(fv);
			    	
			    	sMap.put(value.getTileTag(), list);
			    }
		    }
		    
		    
		}
		
	
		long r = 0;
		long s = 0;
		long  w = 0;
		// Sort the List in the Map
		for(java.util.Iterator<String> i = rMap.keySet().iterator();i.hasNext();){
			
//			TemporalComparator comp = new TemporalComparator();
			String obj = i.next();
//			Collections.sort(rMap.get(obj),comp);
			r += rMap.get(obj).size();
			
		}
		
		for(java.util.Iterator<String> i = sMap.keySet().iterator();i.hasNext();){
			
//			TemporalComparator comp = new TemporalComparator();
			String obj = i.next();
//			Collections.sort(sMap.get(obj),comp);
			s += sMap.get(obj).size();
			
		}
		w = s + r;
		rCount.add(r);
		sCount.add(s);
		wCount.add(w);
		

		
		for(java.util.Iterator<String> obj = rMap.keySet().iterator();obj.hasNext();){
			
			String i = obj.next();
			
			
			if(sMap.containsKey(i)){
				
				
				for(int j = 0;j < rMap.get(i).size();j++){
					
					FlickrValueWithMultiFeature value1 = rMap.get(i).get(j);
					
					
					tCompareCount++;
					for(int k = 0; k < sMap.get(i).size();k++){
						FlickrValueWithMultiFeature value2 = sMap.get(i).get(k);
							
						tCompareCount++;
						if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
							
							sCompareCount++;
							if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
								
								oCompareCount++;
								if(FlickrSimilarityUtil.TextualSimilarity(value1, value2)){
									
									List<String> itext = new ArrayList<String>(Arrays.asList(value1.getTiles().split(",")));
									List<String> jtext = new ArrayList<String>(Arrays.asList(value2.getTiles().split(",")));
									
									jtext.retainAll(itext);
									
									long ridA = value1.getId();
						            long ridB = value2.getId();
						            if (ridA < ridB) {
						                long rid = ridA;
						                ridA = ridB;
						                ridB = rid;
						            }
								
					            
//						            if(i == Integer.parseInt(jtext.get(0))){
						            if(i.toString().equals(jtext.get(jtext.size()-1))){
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

		
		rMap.clear();
		sMap.clear();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("clean up");
		
		long rMax = 0;
		long rMin = 1000000;
		long rC =0;
		System.out.println("R data set");
		for(long i : rCount){
			System.out.println(i);
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
//			System.out.println(i);
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

