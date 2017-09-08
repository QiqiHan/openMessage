package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by H77 on 2017/5/21.
 */
public class WriteBuffer {
    //记录Buffer偏移 大小
    private byte[] buffer = null;
    private int offset = 0;
    private int size = 0;
    //Message编码后的内容
    private byte[] content = null;
    //写的线程
    String topic = null;
    private List<Message> messages;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public WriteBuffer(int size){
        buffer = new byte[1024*140];
        content = new byte[1024*120];
        this.size = 1024*140;
        messages = new ArrayList<>(50);
    }

    public synchronized void putMessage(Message message){
        if(messages.size() < 30){
            messages.add(message);
        }else{
            int length = this.writeMessages(messages);
            if(length < messages.size()) {
                ArrayList<Message> lists = new ArrayList<Message>(600);
                for (int i = length ; i < messages.size() ; i++){
                    lists.add(messages.get(i));
                }
                lists.add(message);
                messages.clear();
                messages.addAll(lists);
            }else {
                messages.clear();
                messages.add(message);
            }
        }
    }
    public  int writeMessages(List<Message> bytesMessages){
        for(int i = 0 ; i< bytesMessages.size() ; i++){
            BytesMessage bytesMessage = (BytesMessage)bytesMessages.get(i);
            if(!write(bytesMessage))  return i;
        }
        return bytesMessages.size();
    }

    public  boolean  write(BytesMessage bytesMessage){
        int length = EncodeUtil.encodeByteMessage(bytesMessage,content);
        //如果缓存满了 将缓存持久化
        if((length+offset) >= size){
            MessageByte message = new MessageByte(buffer,offset);
            ThreadMap.files.get(topic).addMessageByte(message);
            offset = 0;
            buffer = new byte[1024*140];
            //重新初始
            return false;
        }
        //将编码结果BytesMessage 存到缓存
        for(int i = 0 ; i < length ; i++){
            buffer[offset++] = content[i];
        }
        return true;
    }
    public int getOffset() {
        return offset;
    }
    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void flush(){
        MessageByte message = new MessageByte(buffer,offset);
        ThreadMap.files.get(topic).addMessageByte(message);
    }
}
