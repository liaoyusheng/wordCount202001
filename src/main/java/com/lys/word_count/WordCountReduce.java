package com.lys.word_count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for (Text value : values) {
            num += Integer.parseInt(value.toString());
        }
        context.write(key, new Text(num + ""));
    }
}
