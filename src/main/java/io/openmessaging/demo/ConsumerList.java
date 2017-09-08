
package io.openmessaging.demo;

import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by guojianpeng on 2017/5/22.
 */

public class ConsumerList {

    public static Map<String,List<DefaultPullConsumer>> subscirberList
            = new ConcurrentHashMap<>();

//    public static Map<String,List<DefaultPullConsumer>> consumers = new ConcurrentHashMap<>();
//
//    public static Map<String,RandomAccessFile>  files = new ConcurrentHashMap<>();
//
//    public static Map<String,ReadThread> threads = new ConcurrentHashMap<>();
//
//    private static AtomicBoolean isStart = new AtomicBoolean(false);
//
   public static void addTopicInfo(List<String> buckets , DefaultPullConsumer consumer,String storePath){

        try {
            for (String bucket : buckets) {
                if (subscirberList.get(storePath+bucket) == null) {
                    List<DefaultPullConsumer> subscribes = new ArrayList<>(30);
                    subscribes.add(consumer);
                    if (subscirberList.get(storePath+bucket) == null) {
                        subscirberList.put(storePath+bucket, subscribes);
                    } else {
                        subscirberList.get(storePath+bucket).add(consumer);
                    }
                }
            }
        }catch (Exception e){
            //暂时先这样，后面换日志
            e.printStackTrace();
        }
    }

//    public static void setUpThread(){
//        if(threadBoss == null) {
//            synchronized (ConsumerList.class) {
//                if (threadBoss == null) {
//                    ArrayList<RandomAccessFile> file = new ArrayList<>(100);
//                    ArrayList<String> topic = new ArrayList<>(100);
//                    for (String key : files.keySet()) {
//                        topic.add(key);
//                    }
//                    for (RandomAccessFile rf : files.values()) {
//                        file.add(rf);
//                    }
//                    threadBoss = new ReadThreadBoss(file, topic);
//                    Thread thread = new Thread(threadBoss);
//                    thread.start();
//                }
//            }
//        }
//
//
//    }

//    public static void setUpThread(){
//        if(isStart.compareAndSet(false,true)){
//            for (String rf : files.keySet()) {
//                    ReadThread readThread = new ReadThread(files.get(rf));
//                    List<DefaultPullConsumer> consumers = subscirberList.get(rf);
//                    for(DefaultPullConsumer defaultPullConsumer:consumers){
//                        readThread.addQueue(defaultPullConsumer.getCache());
//                    }
//                    Thread thread = new Thread(readThread);
//                    thread.start();
//            }
//        }
//    }
//    public static void wakeUpConsumer(String topic){
//        try {
//            for (DefaultPullConsumer consumer : subscirberList.get(topic)) {
//                DefaultPullConsumer consumer1 = consumer;
//                //consumer.awake(1);
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    public static List<DefaultPullConsumer> getBucketConsumers(String bucket){
//        return subscirberList.get(bucket);
//    }
}

