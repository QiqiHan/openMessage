package io.openmessaging.demo;

import io.openmessaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.*;

public class DefaultProducer  implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
//    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;

//    private Map<String,WriteBuffer> buffers = new HashMap<>();

    private byte[] content = new byte[1024*6];

    static Logger logger = LoggerFactory.getLogger(ReadThread.class);

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {

        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }

        //这地方要考虑消息持久化，持久化后的顺序问题
        try {
            String filePrefix = (topic != null ? topic : queue);
            String directory = properties.getString("STORE_PATH") + "/" + filePrefix;

            try {
                int index = EncodeUtil.encodeByteMessage((BytesMessage) message, content);
                MappedWriteBuffer buffer = ThreadMap.getMappedByteBuffer(directory);
                buffer.putContent(content,0,index);
            }catch (Exception e){
                e.printStackTrace();
            }

//            int index = Integer.parseInt(message.properties().getString("index"));
//            ThreadMap.setUpWriteThread(directory+filePrefix);
//            if(ThreadMap.buffers.get(directory+filePrefix) == null) {
//                ThreadMap.setUpBuffer(directory+filePrefix);
//            }
//            ThreadMap.buffers.get(directory+filePrefix).putMessage(message);
//            ThreadMap.setUpBuffer(directory+filePrefix);
//            int length = EncodeUtil.encodeByteMessage((BytesMessage)message,content);
//            byte[]  bytes = new byte[length];
//            int i = 0;
//            while(i < length){
//                bytes[i] = content[i];
//                i++;
//            }
//            MessageByte messagebyte = new MessageByte(bytes,bytes.length);
//            ThreadMap.files.get(directory+filePrefix).addMessageByte(messagebyte);
//            MessageByte messagebyte = new MessageByte(content,length);
//            ThreadMap.files.get(directory+filePrefix).addMessageByte(messagebyte);
//            if(buffers.get(filePrefix) == null){
//                if(topic==null) {
//                    WriteBuffer buffer = new WriteBuffer(2048);
//                    buffer.setTopic(directory + filePrefix);
//                    buffers.put(filePrefix, buffer);
//                    buffer.putMessage(message);
//                }else{
//                    WriteBuffer buffer = new WriteBuffer(2048*10);
//                    buffer.setTopic(directory + filePrefix);
//                    buffers.put(filePrefix, buffer);
//                    buffer.putMessage(message);
//                }
//            }else{
//                WriteBuffer buffer = buffers.get(filePrefix);
//                buffer.putMessage(message);
//            }

//            messageStore.putMessage(topic != null ? topic : queue, message,directory+filePrefix);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
//        for(WriteBuffer buffer : buffers.values()){
//            buffer.flush();
//        }
//        BufferPool.clear();
//        System.gc();
    }
}
