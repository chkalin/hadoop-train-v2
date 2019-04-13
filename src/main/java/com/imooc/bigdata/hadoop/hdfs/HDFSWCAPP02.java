package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 使用HDFS API完成word count统计
 *
 * 需求：统计HDFS上的文件的wc, 然后将结果输出到HDFS
 *
 * 功能拆解
 * #1 读取HDFS上的文件
 * #2 业务处理（词频统计）： 对文件中的每一行数据都要进行业务处理（按照分隔符）==》 mapper
 * #3 将处理结果缓存起来 ==》 context
 * #4 将结果输出到HDFS ==> HDFS API
 */
public class HDFSWCAPP02 {
    public static void main(String[] args) throws Exception{
        //#1 读取HDFS上的文件

        Properties properties = ParamsUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
        //获取hdfs file system
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(), properties.getProperty(Constants.USER));
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        //通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        ImoocMapper mapper = (ImoocMapper)clazz.newInstance();
        //ImoocMapper mapper = new WordCountMapper();
        ImoocContex contex = new ImoocContex();
        while(iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while((line = reader.readLine()) != null){
                // 2)业务处理（词频统计)
                //在业务逻辑处理完后将结果写到Cache中去
                mapper.map(line, contex);
            }
            reader.close();
            in.close();
        }
        //3 将处理结果缓存起来 Map
        Map<Object, Object> contexMap = contex.getCacheMap();
        //#4 将结果输出到HDFS ==> HDFS API
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fs.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));
        //将第三步的缓存输出到out去
        Set<Map.Entry<Object, Object>> entries = contexMap.entrySet();
        for (Map.Entry<Object, Object> entry: entries){
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }
        out.close();
        fs.close();
        System.out.println("词频统计完成...");
    }
}

