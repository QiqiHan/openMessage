package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

import java.util.HashMap;
import java.util.Map;

public class DefaultMessageFactory implements MessageFactory {

    public static Map<String , Integer> count = new HashMap<>();

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
//        Integer num = count.getOrDefault(topic,0);
//        num++;
//        count.put(topic,num);
//        defaultBytesMessage.putProperties("index",String.valueOf(num));
        return defaultBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
//        Integer num = count.getOrDefault(queue,0);
//        num++;
//        count.put(queue,num);
//        defaultBytesMessage.putProperties("index",String.valueOf(num));
        return defaultBytesMessage;
    }
}
