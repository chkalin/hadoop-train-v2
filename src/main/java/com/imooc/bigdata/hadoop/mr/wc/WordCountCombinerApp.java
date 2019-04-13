package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 使用MR统计HDFS上面文件对应的词频
 *
 * Driver: 配置mapper reducer相关属性
 *
 * 提交到本地运行: 开发过程中使用
 *
 * Combiner操作
 */

public class WordCountCombinerApp {

    public static void main (String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "tom");
        System.setProperty("hadoop.home.dir", "F:\\编程\\大数据\\hadoop-2.6.0-cdh5.15.1");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.50.134:8020");


        //创建一个job
        Job job = Job.getInstance(configuration);

        //设置job对应的参数：设置主类
        job.setJarByClass(WordCountCombinerApp.class );

        //设置job对应的参数：设置自定义的mapper和reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //添加combiner的设置即可
        job.setCombinerClass(WordCountReducer.class);

        //设置job对应的参数：mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置job对应的参数：reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出目录已经存在，则先删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.50.134:8020"), configuration, "tom");
        Path outputPath = new Path("/wordcount/output");
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }

        //设置job对应的参数：作业输出和输入的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ?  0 : -1);

    }
}
