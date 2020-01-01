package com.lys.word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCountMain {

    public static void main(String []arg) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf,"WordCountMain");

        job.setJarByClass(WordCountMain.class);
        //指定map
        job.setMapperClass(WordCountMapper.class);
        //自定reduce
        job.setReducerClass(WordCountReduce.class);

        //一个reduce 任务
        job.setNumReduceTasks(1);
        //输入key 类型
        job.setOutputKeyClass(Text.class);
        //输出的value 类型
        job.setOutputValueClass(Text.class);

       String inPath="hdfs://192.168.80.103:8020/user/wordCountIn/";
       String outPath="hdfs://192.168.80.103:8020/user/wordCountOut";


        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.out.println(job.waitForCompletion(true)?0:1);

    }
}
