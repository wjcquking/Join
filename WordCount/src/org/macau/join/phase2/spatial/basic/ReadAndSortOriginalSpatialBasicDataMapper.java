package org.macau.join.phase2.spatial.basic;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.join.phase2.ValueSelfJoin;
import org.macau.token.TokenRank;
import org.macau.token.tokenizer.Tokenizer;
import org.macau.token.tokenizer.TokenizerFactory;
import org.macau.util.FuzzyJoinConfig;
import org.macau.util.FuzzyJoinUtil;
import org.macau.util.SimilarityUtil;

public class ReadAndSortOriginalSpatialBasicDataMapper extends
	Mapper<Object, Text, IntWritable, ValueSelfJoin> {

	private int[] dataColumns;
	protected final Text token = new Text();
	private Tokenizer tokenizer;
	
	/**
	* run once at the start of the map task
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
	
	
	private final IntWritable outputKey = new IntWritable();
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
		
		
		// use the number stands for the textual
		Collection<Integer> tokensRanked = TokenRank.getTokenRanks(tokens);
		
		int prefixLength = SimilarityUtil.getPrefixLength(tokensRanked.size(), SimilarityUtil.threashold);
		
		
		outputValue.setRID(id);
		outputValue.setTokens(tokensRanked);
		
		int position = 0;
		
		for (Integer token : tokensRanked) {
		    if (position < prefixLength) {
		    	
		    	outputKey.set(token);
		    	context.write(outputKey, outputValue);
		    	
		    } else {
		        break;
		    }
		    position++;
		}
	}
}