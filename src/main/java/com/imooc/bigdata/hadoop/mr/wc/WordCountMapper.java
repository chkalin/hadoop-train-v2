package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * keyIn: Map任务读数据的key类型， offset， 是每行数据起始位置的偏移量， Long
 * valueIn: Map任务读数据的value类型， 其实就是一行行字符 Long
 *
 * hello world welcome
 * hello hello
 *
 * keyOut: map方法自定义实现输出的key的类型 string
 * valueOut: map方法自定义输出的value的类型 integer
 *
 * （hello，3）
 * Java类型：          Long            string      integer
 * Hadoop自定义类型：  LongWritable    text        IntWritable
 */

public class WordCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(" ");


        for(String word : words){
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
