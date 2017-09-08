package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
//    private Map<String,Integer> offsetList = new HashMap<>();
    private Map<String,Long> offsetList = new HashMap<>();
    private int currentIndex = 0;
    private int[] index ;
    private int fileCount = 0;
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override
    public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null ;
        }else {
            while(true){

                long offset = 0L;
                if(currentIndex == bucketList.size()){
                    currentIndex = 0;
                }

                if(index[currentIndex] == -1){
                    currentIndex++;
                    continue;
                }

                String topic = bucketList.get(currentIndex);
                if(offsetList.get(topic) == null){
                    offsetList.put(topic,0L);
                }else{
                    offset = offsetList.get(topic);
                }

                ReadThread readThread = ThreadMap.setUpReadThread(properties.getString("STORE_PATH") + "/"+topic);
//                if( this!= readThread.getConsumer()) {
//                    if (!readThread.trySetConsumer(this)) {
//                        try {
//                            Thread.sleep(10);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        currentIndex ++ ;
//                        continue;
//                    }
//                }

                MessageDTO messageDTO = readThread.read(offset);
                if (messageDTO == null) {
                    fileCount ++;
                    if(fileCount==bucketList.size()){
                        return null;
                    }else{
                        index[currentIndex] = -1;
                        currentIndex++;
                    }
                } else {
                    if(messageDTO.getMessage() == null){
                        currentIndex++;
                    }else {
                        offsetList.put(topic, messageDTO.getOffset());
                        return messageDTO.getMessage();
                    }
                }

            }

        }
//            int index = 0;
//
//            while (true) {
//
//                String bucket = bucketList.get(index);
//
//                if (!offsetList.containsKey(bucket)) {
//                    offsetList.put(bucket, 0);
//                }
//                if (messageStore.containsCache(bucket)) {
//                    ArrayList<Message> messages = messageStore.getCache(bucket);
//                    if (messages != null) {
//                        int offset = offsetList.get(bucket);
//                        if(messages.size() > offset){
//                            offsetList.put(bucket,offset+1);
//                            return messages.get(offset);
//                        } else {
//                            continue;
//                        }
//                    }
//                } else {
//                    ArrayList<Message> queue = new ArrayList<>(1024);
//                    messageStore.addCache(bucket, queue);
//                    ReadThread readThread = new ReadThread(properties.getString("STORE_PATH") + "/" + bucket, queue);
//                    Thread thread = new Thread(readThread);
//                    thread.start();
//                }
//
//                if (index < (bucketList.size() - 1)) {
//                    index++;
//                } else {
//                    index = 0;
//                }
//            }
//        int checkNum = 0;
//        while (++checkNum <= bucketList.size()) {
//            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
//            Message message = messageStore.pullMessage(queue, bucket);
//            if (message != null) {
//                return message;
//            }
//        }
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);

        ConsumerList.addTopicInfo(bucketList,this,properties.getString("STORE_PATH") + "/");

        index = new int[bucketList.size()];

        for(int i = 0 ; i<index.length ; i++){
            index[i] = i;
        }
    }


}
