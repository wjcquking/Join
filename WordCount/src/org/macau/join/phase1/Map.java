/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package org.macau.join.phase1;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.macau.token.tokenizer.Tokenizer;
import org.macau.token.tokenizer.TokenizerFactory;
import org.macau.util.FuzzyJoinConfig;
import org.macau.util.FuzzyJoinUtil;
import org.macau.util.SimilarityUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



/**
 * @author rares
 * 
 *         KEY1: not used
 * 
 *         VALUE1: record (e.g.,
 *         "unused_attribute:RID:unused_attribute:join_attribute:unused_attribute"
 *         )
 * 
 *         KEY2: token
 * 
 *         VALUE2: count
 * 
 */
public class Map extends
Mapper<Object, Text, Text, IntWritable> {

    private int[] dataColumns;
    private final IntWritable one = new IntWritable(1);
    protected final Text token = new Text();
    private Tokenizer tokenizer;

    /**
     * once at the start of the map task
     */
    
    protected void setup(Context context) throws IOException, InterruptedException {
        tokenizer = TokenizerFactory.getTokenizer("Word",
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX, //"_"
                FuzzyJoinConfig.TOKEN_SEPARATOR);//"_"
        
        
//        dataColumns = FuzzyJoinUtil.getDataColumns(job.get(
//                FuzzyJoinConfig.RECORD_DATA_PROPERTY,
//                FuzzyJoinConfig.RECORD_DATA_VALUE));
        //this is the data column in the recored
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

    public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
    	
    	//the the tokens from the record
        Collection<String> tokens = getTokens(value);

        //and_1 means there is one and in the text
        //and_2 means there are two "and" in the text
        //they are different
        for (String tokenString : tokens) {
        	token.set(tokenString);
        	context.write(token, one);
        }
    }
}
