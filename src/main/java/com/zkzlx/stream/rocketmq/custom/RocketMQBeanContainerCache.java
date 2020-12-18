package com.zkzlx.stream.rocketmq.custom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.StringUtils;

/**
 * Gets the beans configured in the configuration file
 *
 * @author junboXiang
 */
public final class RocketMQBeanContainerCache {

    private static  final Map<String, Object> BEANS_CACHE = new ConcurrentHashMap<>();

    public static <T> T getBean(String beanName,Class<T> clazz) {
       return getBean(beanName,clazz,null);
    }

    public static <T> T getBean(String beanName,Class<T> clazz,T defaultObj) {
        if(StringUtils.isEmpty(beanName)){
            return defaultObj;
        }
        Object obj = BEANS_CACHE.get(beanName);
        if(null == obj){
            return defaultObj;
        }
        if(clazz.isAssignableFrom(obj.getClass())){
            return (T) obj;
        }
        return defaultObj;
    }


    static void putBean(String beanName, Object beanObj) {
        BEANS_CACHE.put(beanName, beanObj);
    }

}
