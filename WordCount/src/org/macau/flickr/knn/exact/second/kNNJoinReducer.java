package org.macau.flickr.knn.exact.second;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.macau.flickr.knn.exact.first.PartitionJob;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrPartitionValueComparator;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.spatial.Distance;
import java.util.List;

public class kNNJoinReducer extends
	Reducer<IntWritable,FlickrPartitionValue,FlickrPartitionValue,Text>{

	private FlickrPartitionValue outputKey = new FlickrPartitionValue();
	private Text outputValue = new Text();

//	private static List<kNNPartition> S_SortPartition = new ArrayList<kNNPartition>();
//	private static kNNPartition R_Partition = new kNNPartition();
	
	protected void setup(Context context) throws IOException, InterruptedException {
	
		System.out.println("kNN Join Reducer Starts");
		
		
	}
	
	
	//the most reasonable reason is the I forget to clear the value of R or S
	public void reduce(IntWritable key,Iterable<FlickrPartitionValue> values, Context context)
		throws IOException,InterruptedException{
		
		kNNPartition R_Partition = new kNNPartition();
		R_Partition.getFlickrPartitionValueList().clear();
		List<kNNPartition> S_SortPartition = new ArrayList<kNNPartition>();
		
		int s_Count = 0;
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			S_SortPartition.add(new kNNPartition(kNNJoinJob.S_Partition[i]));
			S_SortPartition.get(i).setCount(0);
			S_SortPartition.get(i).getFlickrPartitionValueList().clear();
		}
		
//		System.out.println("size 0=:" + S_SortPartition.size());
//		Map<Integer, Double> keyfreqs = new HashMap<Integer, Double>();

//		List<FlickrPartitionValue> R_List = new ArrayList<FlickrPartitionValue>();
		
		/*
		for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
			
			double distance = Distance.GreatCircleDistance(PartitionJob.R_Partition[key.get()].getLat(), PartitionJob.R_Partition[key.get()].getLon(), PartitionJob.S_Partition[i].getLat(), PartitionJob.S_Partition[i].getLon());
			keyfreqs.put(i, distance);
			
		}
		
		ArrayList<Entry<Integer,Double>> SPartition = new ArrayList<Entry<Integer,Double>>(keyfreqs.entrySet());  
		
		Collections.sort(SPartition, new Comparator<Map.Entry<Integer, Double>>() {  
			
			public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {  
				return Double.compare(o1.getValue(), o2.getValue());  
			}  
			
		});
		
		for(Entry<Integer,Double> e : SPartition) {
			System.out.println(e.getKey() + "::::" + e.getValue());
		}
		*/
		
		//parse Pi(R) and Si
		for(FlickrPartitionValue val : values){
			s_Count++;
			//R set
			if(val.getDataset() == FlickrSimilarityUtil.R_tag){
				if(R_Partition.getFlickrPartitionValueList().size() == 0){
					
					R_Partition = new kNNPartition(kNNJoinJob.R_Partition[val.getPid()]);
					R_Partition.setCount(0);
					
				}
//				System.out.println("val" + val.getLat() + " " + val.getLon());
				R_Partition.getFlickrPartitionValueList().add(new FlickrPartitionValue(val));
				R_Partition.setCount(R_Partition.getCount()+1);
				
			}else{
				//S set: 1
				//sortParitionMap.get(val.getPid()).add(val);
				
				if(S_SortPartition.get(val.getPid()).getCount() == 0){
					
					//S_SortPartition.add(new kNNPartition(val.getPid(),FlickrSimilarityUtil.REDUCER_NUMBER));
					S_SortPartition.get(val.getPid()).getkNNDistance().add(val.getDistance());
					
					S_SortPartition.get(val.getPid()).getFlickrPartitionValueList().add(new FlickrPartitionValue(val));
					S_SortPartition.get(val.getPid()).setCount(S_SortPartition.get(val.getPid()).getCount()+1);
					S_SortPartition.get(val.getPid()).setMinDistance(val.getDistance());
					S_SortPartition.get(val.getPid()).setMaxDistance(val.getDistance());
					
				}else{
					
					if(S_SortPartition.get(val.getPid()).getkNNDistance().size() < kNNUtil.k){
						S_SortPartition.get(val.getPid()).getkNNDistance().add(val.getDistance());
						
						Collections.sort(S_SortPartition.get(val.getPid()).getkNNDistance());
						
					}else if(S_SortPartition.get(val.getPid()).getkNNDistance().get(kNNUtil.k-1)< val.getDistance()){
						S_SortPartition.get(val.getPid()).getkNNDistance().remove(kNNUtil.k -1);
						S_SortPartition.get(val.getPid()).getkNNDistance().add(val.getDistance());
						Collections.sort(S_SortPartition.get(val.getPid()).getkNNDistance());
						
					}
					//put the val to the partition
					S_SortPartition.get(val.getPid()).getFlickrPartitionValueList().add(new FlickrPartitionValue(val));
					S_SortPartition.get(val.getPid()).setCount(S_SortPartition.get(val.getPid()).getCount()+1);
					
					if(S_SortPartition.get(val.getPid()).getMaxDistance()< val.getDistance()){
						S_SortPartition.get(val.getPid()).setMaxDistance(val.getDistance());
					}
					
					if(S_SortPartition.get(val.getPid()).getMinDistance() > val.getDistance()){
						S_SortPartition.get(val.getPid()).setMinDistance(val.getDistance());
					}
				}
			}
		}
		
		System.out.println("size 7 : " + S_SortPartition.size());
		for(int i = 0,len = S_SortPartition.size(); i < len;i++){
			if(S_SortPartition.get(i).getCount() == 0){
//				S_SortPartition.get(i).getFlickrPartitionValueList().clear();
				S_SortPartition.remove(i);
				--len;
				--i;
			}
		}
		System.out.println("size " + S_SortPartition.size());
//		for(int i = 0; i < S_SortPartition.size();i++){
////			System.out.println(R_Partition.getLat()+ "ii"+R_Partition.getLon()+"wid : " + S_SortPartition.get(i).getLat());
////			System.out.println(R_Partition.getCount() + "rr" + S_SortPartition.get(i).getCount());
//		}
		double theta = kNNJoinFunction.boundingkNN(R_Partition, S_SortPartition);
		
		System.out.println("theta" + theta);
		
		System.out.println("S Count" + s_Count);
		
		System.out.println("size of R : " + R_Partition.getFlickrPartitionValueList().size());
		for(FlickrPartitionValue r: R_Partition.getFlickrPartitionValueList()){
			
			outputKey = new FlickrPartitionValue(r);
			
			List<FlickrPartitionValue> topList = new ArrayList<FlickrPartitionValue>();
			
			double r_Pr = Distance.GreatCircleDistance(r.getLat(), r.getLon(), R_Partition.getLat(), R_Partition.getLon());
			
			int  count = 0;
			for(kNNPartition s_Partition: S_SortPartition){
				
				count += s_Partition.getCount();
				
//				System.out.println("oo" + r.getLat()+" "+r.getLon()+" "+s_Partition.getLat()+" "+s_Partition.getLon() );
				double r_Ps = Distance.GreatCircleDistance(r.getLat(), r.getLon(), s_Partition.getLat(), s_Partition.getLon());
				
				double Pr_Ps = Distance.GreatCircleDistance(R_Partition.getLat(), R_Partition.getLon(), s_Partition.getLat(), s_Partition.getLon());
				double distance_HP;
				
				if(Pr_Ps == 0){
					
					distance_HP = 0;
					
				}else{
					
					distance_HP = (Math.pow(r_Ps,2) - Math.pow(r_Pr, 2))/(2*Pr_Ps);
				}
				
				if(distance_HP < theta){
//					System.out.println(Math.pow(r_Ps,2)+ "tt "+ Math.pow(r_Pr, 2)+ "tt"+ 2*Pr_Ps);
//					System.out.println(distance_HP + " dd " +theta);
					double minValue = (s_Partition.getMinDistance() > r_Ps ? s_Partition.getMinDistance() : r_Ps);
					
					for(FlickrPartitionValue s : s_Partition.getFlickrPartitionValueList()){
						
						//According the theorem 2 , the range of the answer 
						if(s.getDistance() > minValue){
						
							double distance =  Distance.GreatCircleDistance(r.getLat(),r.getLon(),s.getLat(),s.getLon());
							if(distance < theta){
								if(topList.size() < kNNUtil.k){
									
									topList.add(s);
									
									if(topList.size() == kNNUtil.k){
										Collections.sort(topList,new FlickrPartitionValueComparator());
									}
									
								}else if(topList.get(kNNUtil.k-1).getDistance() > distance){
									
									topList.remove(kNNUtil.k-1);
									topList.add(s);
									Collections.sort(topList,new FlickrPartitionValueComparator());
									theta = topList.get(kNNUtil.k -1).getDistance();
									
								}
							}
						}
					}
				}
			}
			System.out.println("size S "+ count);
			String result = "";
			for(FlickrPartitionValue fpv : topList){
				result += fpv.toString() + "%";
			}
			outputValue.set(result);
			
//			for(kNNPartition k : S_SortPartition){
//				k.getFlickrPartitionValueList().clear();
//			}
//			System.out.println("the topList" + topList.size());
//			System.out.println("final " + outputKey + "-----" + outputValue);
			context.write(outputKey, outputValue);
		}
		
		System.out.println(R_Partition.getCount());
		
	}
	
}
