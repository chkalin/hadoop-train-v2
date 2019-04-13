package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用MR统计HDFS上面文件对应的词频
 *
 * Driver: 配置mapper reducer相关属性
 *
 * 提交到本地运行: 开发过程中使用
 *
 * 使用本地文件进行统计，然后结果统计输出到本地路径
 */

public class WordCountLocalApp {

    public static void main (String[] args) throws Exception {

        //System.setProperty("hadoop.home.dir", "F:\\编程\\大数据\\hadoop-2.6.0-cdh5.15.1");
        Configuration configuration = new Configuration();


        //创建一个job
        Job job = Job.getInstance(configuration);

        //设置job对应的参数：设置主类
        job.setJarByClass(WordCountLocalApp.class );

        //设置job对应的参数：设置自定义的mapper和reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置job对应的参数：mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置job对应的参数：reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置job对应的参数：作业输出和输入的路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        //提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ?  0 : -1);

    }
}
