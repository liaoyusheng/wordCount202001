package com.lys.word_count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper  extends Mapper<Object, Text,Text,Text> {
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
        String s=value.toString();
        StringTokenizer st=new StringTokenizer(s);

        while(st.hasMoreElements()){
          String sw=st.nextToken();
          //去除符号
          String word=sw.replace(",","").replace(":","").replace(";","")
                  .replace("?","").replace("!","").replace(".","")
                  .replace("“","").replace("”","");
          context.write(new Text(word),new Text("1"));
        }

    }
}
