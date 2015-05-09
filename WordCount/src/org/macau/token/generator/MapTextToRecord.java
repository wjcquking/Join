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

package org.macau.token.generator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.macau.util.FuzzyJoinConfig;
import org.macau.util.FuzzyJoinDriver;
import org.macau.util.SimilarityUtil;



/**
 * 
 * @author hadoop
 *
 * this map is used to preprocessed the data by adding id in front of the record
 * this map is in the set up job
 */
public class MapTextToRecord extends
Mapper<Object, Text, Text, NullWritable> {

    private int noRecords;
    private Text record = new Text();
    private long rid = 1;

    protected void setup(Context context) throws IOException, InterruptedException {
    	//noRecords = job.getInt(FuzzyJoinDriver.DATA_NORECORDS_PROPERTY, -1);
        noRecords = SimilarityUtil.noRecords;
    }

    /**
     * KEY1: object
     * VALUE1: record
     * KEY2: rid:record
     * VALUE2:null
     * @throws InterruptedException 
     * 
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (noRecords == -1 || rid <= noRecords) {
            record.set("" + (rid++) + FuzzyJoinConfig.RECORD_SEPARATOR
                    + value.toString());
            context.write(record, NullWritable.get());
        }
    }
}