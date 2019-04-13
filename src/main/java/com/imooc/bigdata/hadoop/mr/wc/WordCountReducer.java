package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    /**
     *（hello，1）（hello，1）（hello，1）
     *（welcome，1）（welcome，1）（welcome，1）
     * （word，1）
     *
     * map的输出到reduce端，是按照相同的Key分发到一个reduce上执行
     *
     * reduce 1: （hello，1）（hello，1）（hello，1）==> (hello, <1,1,1>)
     * reduce 2: （welcome，1）（welcome，1）（welcome，1）==> (welcome, <1,1,1>)
     * reduce 3: （word，1）==> (word, <1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();

        // iterator = <1,1,1>
        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
