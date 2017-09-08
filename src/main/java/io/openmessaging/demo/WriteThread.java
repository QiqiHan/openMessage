package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mac on 17/5/18.
 */
public class WriteThread implements Runnable{

    private  LinkedBlockingQueue<MessageByte> messages = new LinkedBlockingQueue<>();
    private  RandomAccessFile randomAccessFile = null;
    public void setRandomAccessFile(RandomAccessFile randomAccessFile){
        this.randomAccessFile = randomAccessFile;
    }
    public void addMessageByte(MessageByte messageByte){
        try {
            messages.put(messageByte);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                MessageByte message = messages.take();
                long fileLength = randomAccessFile.length();
                randomAccessFile.seek(fileLength);
                randomAccessFile.write(message.getBuffer(),0,message.getOffset());
//                BufferPool.returnBuffer(message.getBuffer());
                message.setBuffer(null);
                message = null;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


}
