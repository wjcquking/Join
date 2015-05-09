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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.macau.join.OnePhaseTokenOrdering;
import org.macau.join.OnePhaseTokenOrdering.TokenLocalSumCombiner;
import org.macau.join.OnePhaseTokenOrdering.TokenSumSortReducer;
import org.macau.util.FuzzyJoinDriver;
import org.macau.util.SimilarityUtil;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author hadoop
 * 对原数据进行处理，加上ID
 */
public class RecordBuild {
	
	/**
	 * 
	 * @param conf
	 * @return
	 * @throws Exception
	 * if the job finished, then return true
	 */
	public static boolean RecordsBuild(Configuration conf) throws Exception{
		
		Job job = new Job(conf, "Records Build");
		job.setJarByClass(RecordBuild.class);
		job.setMapperClass(MapTextToRecord.class);
//		job.setCombinerClass(TokenLocalSumCombiner.class);
//		job.setReducerClass(TokenSumSortReducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		
		FileInputFormat.addInputPath(job, new Path(SimilarityUtil.recordsInputPath));
		FileOutputFormat.setOutputPath(job, new Path(SimilarityUtil.recordsGeneratePath));
		
		if(job.waitForCompletion(true)){
			return true;
		}
		else 
			return false;
	}
    public static void main(String[] args) throws IOException {
        //
        // setup job
        //
//        JobConf job = new JobConf();
//
//        System.out.println(Arrays.toString(args));
//
//        new GenericOptionsParser(job, args);
//        job.setJobName(RecordBuild.class.getSimpleName());
//
//        //job.setMapperClass(MapTextToRecord.class);
//        job.setLong("mapred.min.split.size", Long.MAX_VALUE);
//        job.setNumReduceTasks(0);
//
//        //
//        // set input & output
//        //
//        
//        String dataDir = job.get(FuzzyJoinDriver.DATA_DIR_PROPERTY);
//        if (dataDir == null) {
//            throw new UnsupportedOperationException("ERROR: "
//                    + FuzzyJoinDriver.DATA_DIR_PROPERTY + " not set");
//        }
//        String suffix = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
//        if (!suffix.isEmpty()) {
//            if (suffix.indexOf(FuzzyJoinDriver.SEPSARATOR) >= 0) {
//                throw new UnsupportedOperationException("ERROR: "
//                        + FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY
//                        + " contains more that one value");
//            }
//            suffix = "." + suffix;
//        }
//        String raw = job.get(FuzzyJoinDriver.DATA_RAW_PROPERTY,
//                FuzzyJoinDriver.DATA_RAW_VALUE);
//        FileInputFormat.addInputPath(job, new Path(dataDir + "/raw" + suffix
//                + "-000/" + raw));
//        Path outputPath = new Path(dataDir + "/recordsbulk" + suffix + "-000");
//        FileOutputFormat.setOutputPath(job, outputPath);
//        FileSystem.get(job).delete(outputPath, true);
//
//        //
//        // run
//        //
//        FuzzyJoinDriver.run(job);
    }
}
