package io.openmessaging.demo;

/**
 * Created by mac on 17/5/19.
 */
public class Constants {

    public final static String STORE_PATH = System.getProperty("store.path", "/Users/mac/Desktop/test1");
    public final static int PRO_NUM = Integer.valueOf(System.getProperty("pro.num", "30"));
    public final static int CON_NUM = Integer.valueOf(System.getProperty("con.num", "10"));
    public final static String PRO_PRE = System.getProperty("pro.pre","PRODUCER_");
    public final static int PRO_MAX = Integer.valueOf(System.getProperty("pro.max","400000"));
    public final static String CON_PRE = System.getProperty("con.pre", "CONSUMER_");
    public final static String TOPIC_PRE = System.getProperty("topic.pre", "TOPIC_");
    public final static String QUEUE_PRE = System.getProperty("topic.pre", "QUEUE_");
    public final static int TYPE_LEN = Integer.valueOf(System.getProperty("type.len", "1"));
    public final static int LEN_LEN = Integer.valueOf(System.getProperty("len.len", "4"));
}
