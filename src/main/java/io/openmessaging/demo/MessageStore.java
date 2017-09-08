package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//这个可以理解为一个broker
public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private Map<String,ArrayList<Message>> messageCache = new ConcurrentHashMap<>();


    public void addCache(String bucket ,ArrayList<Message> queue){
        messageCache.put(bucket,queue);
    }

    public boolean containsCache(String bucket){
        return messageCache.containsKey(bucket);
    }

    public ArrayList<Message> getCache(String bucket){
        return messageCache.getOrDefault(bucket,null);
    }


    public synchronized void putMessage(String bucket, Message message,String key) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);

//        if(bucketList.size() < 425){
//            bucketList.add(message);
//        }else{
//            WriteBuffer writeBuffer = ThreadMap.buffers.get(key);
//            int length = writeBuffer.writeMessages(bucketList);
//            if(length < 425) {
//                ArrayList<Message> lists = new ArrayList<Message>(1024);
//                for (int i = length ; i < 425 ; i++){
//                    lists.add(bucketList.get(i));
//                }
//                lists.add(message);
//                bucketList.clear();
//                bucketList.addAll(lists);
//            }else {
//                bucketList.clear();
//                bucketList.add(message);
//            }
//        }
    }

   public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }
}
