package org.macau.join.phase2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.join.phase2.set.ReadAndSortOriginalDataMapper;
import org.macau.join.phase2.set.SimilaritySelfJoinReducer;
import org.macau.join.phase2.spatial.basic.ReadAndSortOriginalSpatialBasicDataMapper;
import org.macau.join.phase2.spatial.basic.SimilaritySelfJoinSpatialBasicReducer;
import org.macau.join.phase2.spatial.cell.ReadAndSortOriginalSpatialCellDataMapper;
import org.macau.join.phase2.spatial.cell.SimilaritySelfJoinSpatialCellReducer;
import org.macau.paper.SimilarityFiltersFactory;
import org.macau.similarity.PartialIntersect;
import org.macau.similarity.SimilarityFilters;
import org.macau.similarity.SimilarityMetric;
import org.macau.spatial.CellPartition;
import org.macau.token.TokenRank;
import org.macau.token.TokenSimilarity;
import org.macau.token.tokenizer.Tokenizer;
import org.macau.token.tokenizer.TokenizerFactory;
import org.macau.util.FuzzyJoinConfig;
import org.macau.util.FuzzyJoinUtil;
import org.macau.util.SimilarityUtil;


public class BasicKernel {
	
	
	public static boolean BasicKernelSpatialBasicJoin(Configuration conf) throws Exception{
		
		Job secondReadJob = new Job(conf,"Basic Kernel Join with Spatial Information");
		secondReadJob.setJarByClass(BasicKernel.class);
		
		secondReadJob.setMapperClass(ReadAndSortOriginalSpatialBasicDataMapper.class);
		//there can add one combiner which can combine the result
		//secondReadJob.setCombinerClass(IntSumReducer.class);
		secondReadJob.setReducerClass(SimilaritySelfJoinSpatialBasicReducer.class);
		
		

		secondReadJob.setMapOutputKeyClass(IntWritable.class);
		secondReadJob.setMapOutputValueClass(ValueSelfJoin.class);
		
		FileInputFormat.addInputPath(secondReadJob, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(secondReadJob, new Path(SimilarityUtil.ridPairsOutputPath));
		
		if(secondReadJob.waitForCompletion(true))
			return true;
		else
			return false;
	}

	public static boolean BasicKernelSpatialCellJoin(Configuration conf) throws Exception{
		
		Job secondReadJob = new Job(conf,"Basic Kernel Join with Spatial Information");
		secondReadJob.setJarByClass(BasicKernel.class);
		
		secondReadJob.setMapperClass(ReadAndSortOriginalSpatialCellDataMapper.class);
		//there can add one combiner which can combine the result
		//secondReadJob.setCombinerClass(IntSumReducer.class);
		secondReadJob.setReducerClass(SimilaritySelfJoinSpatialCellReducer.class);
		
		

		secondReadJob.setMapOutputKeyClass(IntPairWritable.class);
		secondReadJob.setMapOutputValueClass(ValueSelfJoin.class);
		
		FileInputFormat.addInputPath(secondReadJob, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(secondReadJob, new Path(SimilarityUtil.ridPairsOutputPath));
		
		if(secondReadJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
	
	public static boolean BasicKernelJoin(Configuration conf) throws Exception{
		
		Job secondReadJob = new Job(conf,"word read and reorder");
		secondReadJob.setJarByClass(BasicKernel.class);
		
		secondReadJob.setMapperClass(ReadAndSortOriginalDataMapper.class);
		secondReadJob.setReducerClass(SimilaritySelfJoinReducer.class);
		
		secondReadJob.setMapOutputKeyClass(IntWritable.class);
		secondReadJob.setMapOutputValueClass(ValueSelfJoin.class);
		
		FileInputFormat.addInputPath(secondReadJob, new Path(SimilarityUtil.recordsGeneratePath));
		FileOutputFormat.setOutputPath(secondReadJob, new Path(SimilarityUtil.ridPairsOutputPath));
		
		if(secondReadJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
	
	
	
	

	/**
	 * 
	 * @author hadoop
	 * read the original data and reorder the data
	 * use the sort number stands for the data
	 * Input: KEY: null VALUE:Text
	 * output:KEY: IntWritable VALUE:Text(id+text)
	 */
	public static class ReadAndSortOriginalSpatialDataMapper extends
			Mapper<Object, Text, IntPairWritable, ValueSelfJoin> {

		private int[] dataColumns;
	    protected final Text token = new Text();
	    private Tokenizer tokenizer;

	    /**
	     * once at the start of the map task
	     */
	    
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	
	        tokenizer = TokenizerFactory.getTokenizer("Word",
	                FuzzyJoinConfig.WORD_SEPARATOR_REGEX, //"_"
	                FuzzyJoinConfig.TOKEN_SEPARATOR);//"_"
	        
	        dataColumns = SimilarityUtil.dataColumns;
	    }

	    
	    //TOKEN_SEPARATOR-----"_"
	    protected Collection<String> getTokens(Text record) {
	        return tokenizer.tokenize(FuzzyJoinUtil
	                .getData(
	                        record.toString().split(
	                                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
	                        dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
	    }
		
		
		private IntPairWritable outputKey = new IntPairWritable();
		private final ValueSelfJoin outputValue = new ValueSelfJoin();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Collection<String> tokens = getTokens(value);
			
			int id = Integer.parseInt(value.toString().split(":")[0]);

			String positionInfo = value.toString().split(":")[1];
			
			String[] spatialArray = positionInfo.split(",");
			//the position information
			int x = Integer.parseInt(spatialArray[0]);
			int y = Integer.parseInt(spatialArray[1]);
			outputValue.setX(x);
			outputValue.setY(y);
			
			int cellId = CellPartition.getCellId(x, y);
			
			// use the number stands for the textual
			Collection<Integer> tokensRanked = TokenRank.getTokenRanks(tokens);
		
			int prefixLength = SimilarityUtil.getPrefixLength(tokensRanked.size(), SimilarityUtil.threashold);
			

			outputValue.setRID(id);
			
			outputValue.setTokens(tokensRanked);
			
			int position = 0;
			
			for (Integer token : tokensRanked) {
	            if (position < prefixLength) {
	            	outputKey.setToken(token);
	            	
	            	for(Integer cid : CellPartition.getCells(cellId)){
	            		outputKey.setCid(cid);
	            		context.write(outputKey, outputValue);
	            	}
	            	
	            } else {
	                break;
	            }
	            position++;
	        }
		}
	}
	
	
	/**
	 * 
	 * @author hadoop
	 * @param Text 
	 * input: KEY: Text VALUE: 
	 * output:KEY: Text(pairs) VALUE:similarity value
	 */
	public static class SimilarityReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			

			//System.out.println("start phase2 reduce");
			//record all the objects which has the same prefix value
			Set<String> set = new HashSet<String>();
			
			set.clear();
			
			for (Text val : values) {
				//System.out.println("phase 2: reduce---key :" + key.toString() + ",value : " + val.toString());
				set.add(val.toString());
			}
			
			//traverse the map and calculate the similarity of each two objects
			//we can use the positional filter,size filter and suffix filter
			
			//System.out.println("List size is " + list.size());
			Object[] list =  set.toArray();
			for(int i = 0;i < list.length-1;i++){
				String[] iattributes = list[i].toString().split("%");
				
				//get the id of object
				int i_id = Integer.parseInt(iattributes[0]);
		
				for(int j = i+1;j < list.length;j++){
					String[] jattributes = list[j].toString().split("%");
					int j_id = Integer.parseInt(jattributes[0]);
					
					double similarityValue = TokenSimilarity.getTokenSimilarity(iattributes[1], jattributes[1]);
					
					
					if(similarityValue >= 0.5){
						String keyStr= "";
						if(i_id <= j_id){
							keyStr = i_id + "%"+ j_id;
						}else{
							keyStr = j_id + "%"+ i_id;
						}
						
						
						//System.out.println("i_id : " + i_id + "j_id : " + j_id);
						
						key.set(keyStr);
						result.set(similarityValue+"");
						//System.out.println("phase2: reduce-output: key:"+key.toString()+ ",value:" + result.toString());
						context.write(key, result);
					}
				}
			}
			
		}
	}
	
	

	/**
	 * 
	 * @author hadoop
	 * @param Text 
	 * input: KEY: Text VALUE: 
	 * output:KEY: Text(pairs) VALUE:similarity value
	 */
	public static class SimilaritySelfJoinSpatialReducer extends
			Reducer<IntPairWritable, ValueSelfJoin, Text, Text> {
		
		private float similarityThreshold;
	    private SimilarityFilters similarityFilters;
	    // private SimilarityMetric similarityMetric;
	    private final ArrayList<ValueSelfJoin> records = new ArrayList<ValueSelfJoin>();
	    private final Text text = new Text();
	    
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	String similarityName = "jaccard";
	        similarityThreshold = 0.5F;
	        similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
	                similarityName, similarityThreshold);
	    }
	    
	    public void reduce(IntPairWritable key, Iterable<ValueSelfJoin> values,
				Context context) throws IOException, InterruptedException{	
			//System.out.println("basic kernel reduce");
			
			for(ValueSelfJoin value:values){
	            ValueSelfJoin recCopy = new ValueSelfJoin(value);
	            //System.out.println("key:" + key + "value" + value);
	            records.add(recCopy);
			}

	        for (int i = 0; i < records.size(); i++) {
	            ValueSelfJoin rec1 = records.get(i);
	            for (int j = i + 1; j < records.size(); j++) {
	                ValueSelfJoin rec2 = records.get(j);
	                // reporter.incrCounter(Counters.PAIRS_BUILD, 1);
	                
	                if(CellPartition.getDistance(rec1.getX(), rec1.getY(), rec1.getX(), rec2.getY())>SimilarityUtil.distanceThreashold){
	                	continue;
	                }
	                
	                int[] tokens1 = rec1.getTokens();
	                int[] tokens2 = rec2.getTokens();
	                if (!similarityFilters.passLengthFilter(tokens1.length,
	                        tokens2.length)) {
	                    // reporter.incrCounter(Counters.PAIRS_FILTERED, 1);
	                    continue;
	                }

	                PartialIntersect p = SimilarityMetric.getPartialIntersectSize(
	                        tokens1, tokens2, key.getToken());
	                if (!similarityFilters.passPositionFilter(p.intersectSize,
	                        p.posXStop, tokens1.length, p.posYStop, tokens2.length)) {
	                    continue;
	                }

	                if (!similarityFilters.passSuffixFilter(tokens1, p.posXStart,
	                        tokens2, p.posYStart)) {
	                    continue;
	                }

	                float similarity = similarityFilters.passSimilarityFilter(
	                        tokens1, p.posXStop + 1, tokens2, p.posYStop + 1,
	                        p.intersectSize);

	                if (similarity >= similarityThreshold) {
	                    int ridA = rec1.getRID();
	                    int ridB = rec2.getRID();
	                    if (ridA < ridB) {
	                        int rid = ridA;
	                        ridA = ridB;
	                        ridB = rid;
	                    }
	                    
//	                    text.set("" + ridA + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
//	                            + ridB + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
//	                            + similarity);
	                    
	                    text.set("" + ridA + "%" + ridB);
	                    context.write(text, new Text(""+ similarity));
	                }
	            }
	        }
	        records.clear();
		}
	}
	
	
	
		
}
