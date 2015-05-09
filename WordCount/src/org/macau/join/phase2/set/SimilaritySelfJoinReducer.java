package org.macau.join.phase2.set;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.join.phase2.ValueSelfJoin;
import org.macau.paper.SimilarityFiltersFactory;
import org.macau.similarity.PartialIntersect;
import org.macau.similarity.SimilarityFilters;
import org.macau.similarity.SimilarityMetric;


/**
 * 
 * @author hadoop
 * @param Text 
 * input: KEY: Text VALUE: 
 * output:KEY: Text(pairs) VALUE:similarity value
 */

public class SimilaritySelfJoinReducer extends
	Reducer<IntWritable, ValueSelfJoin, Text, Text> {
		
	private float similarityThreshold;
	private SimilarityFilters similarityFilters;
	
	private final ArrayList<ValueSelfJoin> records = new ArrayList<ValueSelfJoin>();
	private final Text text = new Text();
	
	protected void setup(Context context) throws IOException, InterruptedException {
		String similarityName = "jaccard";
		similarityThreshold = 0.5F;
		similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
		        similarityName, similarityThreshold);
	}
	
	public void reduce(IntWritable key, Iterable<ValueSelfJoin> values,
		Context context) throws IOException, InterruptedException{	
	
		for(ValueSelfJoin value:values){
		    ValueSelfJoin recCopy = new ValueSelfJoin(value);
		    records.add(recCopy);
		}
		
		for (int i = 0; i < records.size(); i++) {
		    ValueSelfJoin rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		        ValueSelfJoin rec2 = records.get(j);
		        
		        int[] tokens1 = rec1.getTokens();
		        int[] tokens2 = rec2.getTokens();
		        if (!similarityFilters.passLengthFilter(tokens1.length,
		                tokens2.length)) {
		            // reporter.incrCounter(Counters.PAIRS_FILTERED, 1);
		            continue;
		        }
		
		        PartialIntersect p = SimilarityMetric.getPartialIntersectSize(
		                tokens1, tokens2, key.get());
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
		            
		//            text.set("" + ridA + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
		//                    + ridB + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
		//                    + similarity);
		            
		            text.set("" + ridA + "%" + ridB);
		            context.write(text, new Text(""+ similarity));
		        }
		    }
		}
		records.clear();
	}
}
