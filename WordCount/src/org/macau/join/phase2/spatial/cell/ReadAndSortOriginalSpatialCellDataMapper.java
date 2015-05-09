package org.macau.join.phase2.spatial.cell;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.join.phase2.IntPairWritable;
import org.macau.join.phase2.ValueSelfJoin;
import org.macau.spatial.CellPartition;
import org.macau.token.TokenRank;
import org.macau.token.tokenizer.Tokenizer;
import org.macau.token.tokenizer.TokenizerFactory;
import org.macau.util.FuzzyJoinConfig;
import org.macau.util.FuzzyJoinUtil;
import org.macau.util.SimilarityUtil;

public class ReadAndSortOriginalSpatialCellDataMapper extends
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
		//System.out.println("id---" + id + "cid:"+ cellId);
		
		int position = 0;
		
		for (Integer token : tokensRanked) {
		    if (position < prefixLength) {
		    	outputKey.setToken(token);
		    	
		    	for(Integer cid : CellPartition.getCells(cellId)){
		    		//if(id == 63 || id == 65)
		    			//System.out.println("cid :" + cid);
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
