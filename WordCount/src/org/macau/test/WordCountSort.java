package org.macau.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;

public class WordCountSort {

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString().toLowerCase(); // 全部转为小写字母
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    private static class IntWritableDecreasingComparator extends
            IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) {
        
        String inputDir = "hdfs://localhost:9000/user/hadoop/input";
        String outDirTemp = "/word_count_temp" 
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));;
            
        String outDire = "hdfs://localhost:9000/user/hadoop/output";
        JobConf jobCount = new JobConf(WordCountSort.class);
        try{
            //Job取和部分
            
            jobCount.setJobName("Word Count 3 sum");

            jobCount.setOutputKeyClass(Text.class);
            jobCount.setOutputValueClass(IntWritable.class);

            jobCount.setMapperClass(Map.class);
            jobCount.setCombinerClass(Reduce.class);
            jobCount.setReducerClass(Reduce.class);

            jobCount.setInputFormat(TextInputFormat.class);
            jobCount.setOutputFormat(SequenceFileOutputFormat.class);
            
            // 如果输出目录已经存在，那么先将其删除
            /*FileSystem fstm = FileSystem.get(jobCount);
            Path  outDirTempP = new Path(outDirTemp);
            fstm.delete(outDirTempP, true);*/

            FileInputFormat.setInputPaths(jobCount, new Path(inputDir));
            FileOutputFormat.setOutputPath(jobCount, new Path(outDirTemp));

            JobClient.runJob(jobCount);
            
            //Job排序部分
            
            JobConf jobsort = new JobConf(WordCountSort.class);
            jobsort.setJobName("Word Count 3 sort ");
            
            /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
            jobsort.setMapperClass(InverseMapper.class);
            /* 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。 */
            jobsort.setNumReduceTasks(1);

            jobsort.setOutputKeyClass(IntWritable.class);
            jobsort.setOutputValueClass(Text.class);

            jobsort.setInputFormat(SequenceFileInputFormat.class);
            jobsort.setOutputFormat(TextOutputFormat.class);
            
            /*
             * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 因此我们实现了一个
             * IntWritableDecreasingComparator 类,　 并指定使用这个自定义的 Comparator
             * 类对输出结果中的 key (词频)进行排序
             */
            jobsort.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class);
            
            // 如果输出目录已经存在，那么先将其删除
            FileSystem fstmsort = FileSystem.get(jobsort);
            Path outDirP = new Path(outDire);
            fstmsort.delete(outDirP, true);

            FileInputFormat.setInputPaths(jobsort, new Path(outDirTemp));
            FileOutputFormat.setOutputPath(jobsort, new Path(outDire));

            JobClient.runJob(jobsort);
            
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            //删除临时目录
            FileSystem fstm;
            try {
                fstm = FileSystem.get(jobCount);
                Path outDirP = new Path(outDirTemp);
                fstm.delete(outDirP, true);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        
        
        
    }
}