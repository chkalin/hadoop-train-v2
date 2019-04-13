package com.imooc.bigdata.hadoop.hdfs;

/**
 * 自定义wc实现类
 */

public class WordCountMapper implements ImoocMapper {
    public void map(String line, ImoocContex contex){
        String[] words = line.split(" ");
        for(String word: words) {
            Object value = contex.get(word);
            if(value == null) {
                // 没出现该单词
                contex.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                //取出单词对应次数+1
                contex.write(word, v+1);
            }
            //System.out.println(word +" "+ Integer.parseInt(contex.get(word).toString()));
        }
    }
}
