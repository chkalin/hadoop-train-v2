package com.imooc.bigdata.hadoop.hdfs;

import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;

/**
 *  自定义上下文，其实就是缓存
 */

public class ImoocContex {

    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object, Object> getCacheMap(){
        return cacheMap;
    }
    /**
     * 写数据到缓存中去
     * @param key       单词
     * @param value     词频
     */
    public void write(Object key, Object value){
        cacheMap.put(key, value);
    }

    /**
     * 从缓存中获取值
     * @param key   单词
     * @return      返回词频
     */
    public Object get(Object key){
        return cacheMap.get(key);
    }


}
