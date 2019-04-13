package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce 自定义分区规则
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     *
     * @param text 手机号
     */
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {

        if(phone.toString().startsWith("13")){
            return 0;
        }else if(phone.toString().startsWith("15")){
            return 1;
        } else{
            return 2;
        }
    }
}
