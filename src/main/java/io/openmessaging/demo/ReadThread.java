package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicStampedReference;

import io.openmessaging.PullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by mac on 17/5/19.
 */
public class ReadThread{


    //    private ArrayList<Message> messages;
//    private RandomAccessFile randomAccessFile;
//    private volatile AtomicStampedReference<PullConsumer> consumer = new AtomicStampedReference(null, 0);
    private ReadBuffer readBuffer;
    private int offset;
    private int fileoffset ;
    static Logger logger = LoggerFactory.getLogger(ReadThread.class);
    private byte[] buf = new byte[Constants.LEN_LEN];
    private byte[] contents = new byte[1024*6];
    private int Num = 0;
    private int currentNum = 0;
    public ReadThread(String fileName , int Num){
        try {
//            this.randomAccessFile = new RandomAccessFile(fileName, "r");
            this.readBuffer = new ReadBuffer(fileName);
//            this.messages = messages;
            offset = 0 ;
            this.Num  = Num;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

//    public PullConsumer getConsumer(){
//        return consumer.getReference();
//    }
//
//    public boolean trySetConsumer(PullConsumer pullConsumer){
//        int i = this.consumer.getStamp();
//        return this.consumer.compareAndSet(null,pullConsumer,i,i+1);
//    }
//
//    public void ReleaseConsumer(){
//        PullConsumer pullConsumer = consumer.getReference();
//        int i = this.consumer.getStamp();
//        this.consumer.compareAndSet(pullConsumer,null,i,i+1);
//    }

    public synchronized MessageDTO read(long fileoffset) {
        MessageDTO messageDTO = new MessageDTO();

        try {
            //while(fileoffset<readBuffer.getFileLength()) {
            if(!readBuffer.isMapped(fileoffset,2) && currentNum <Num){
                currentNum ++ ;
                messageDTO.setMessage(null);
                return messageDTO;
            }else {
                if (fileoffset < readBuffer.getFileLength()) {
                    DefaultBytesMessage bytesMessage = new DefaultBytesMessage();
                    long nextPos = readBuffer.getContent(buf, 0, 2, fileoffset);
                    int Alllength = 0;
                    Alllength = ( 0xFF & buf[0] ) << 8;
                    Alllength = Alllength | (buf[1] & 0xFF);
                    if (Alllength <= 0) {
                        return null;
                    }
                    if (!readBuffer.isMapped(fileoffset+2,Alllength) && currentNum <Num){
                        currentNum ++;
                        messageDTO.setMessage(null);
                        return messageDTO;
                    }else{
                        nextPos = readBuffer.getContent(contents, 0, Alllength, nextPos);
                        while (setUpKeyValue(contents, offset, bytesMessage)) ;
                        //组装body
                        //得到判断Body的字节
                        byte bodyType = contents[offset++];
                        //offset++; //直接跳过判断body type那一字节
                        int lengthType = 0 | (bodyType >> 2);
                        int bodyLength = 0;
                        if(lengthType == 0){
                            bodyLength = 0xFF & contents[offset++];
                        } else if(lengthType == 1){
                            bodyLength =  (0xFF & contents[offset++]) << 8;
                            bodyLength =   bodyLength | (contents[offset++] & 0xFF);
                        }

                        byte[] body = new byte[bodyLength];
                        System.arraycopy(contents, offset , body, 0, bodyLength);
                        bytesMessage.setBody(body);
                        //目前 length的大小改成2了
                        fileoffset = fileoffset + Alllength + 2;
                        offset = 0;
                        messageDTO.setMessage(bytesMessage);
                        messageDTO.setOffset(fileoffset);
                        return messageDTO;
                    }
                } else {
                    return null;
                }
                //messages.add(bytesMessage);

                //messages.add(null);
            }
        }catch (Exception e){
            logger.error("error in read ",e);
            return null;
        }

    }


    private  boolean setUpKeyValue(byte[] contents,int start,DefaultBytesMessage bytesMessage){

        //判断类型
        byte type = contents[start++];

        int keyType = type & 3;
        KeyValue keyValue = null;
        if (keyType == 0){
            keyValue= bytesMessage.headers();
        }else if(keyType == 1){
            keyValue= bytesMessage.properties();
//            if(keyValue == null){
//                keyValue = new DefaultKeyValue();
//                bytesMessage.setProperties(keyValue);
//            }
        }else{
            return false;
        }
        String key = "";
        int keyValueType = contents[start++];
        if(keyValueType == 0){
            key = "Topic";
        }else if(keyValueType == 1){
            key = "Queue";
        }else if(keyValueType == 2){
            key = "MessageId";
        }else if(keyValueType == 3) {
            key = "PRO_OFFSET";
        }else {
            //判断长度
            int lengthType = type & 4;
            int keyLength = 0;
            if (lengthType == 0) {
                keyLength = contents[start++] & 0xFF;
            } else {
                keyLength = (0xFF & contents[start++]) << 8;
                keyLength = keyLength | (contents[start++] & 0xFF);
            }
            key = new String(contents, start, keyLength);
            start = start + keyLength;
        }
//        byte value_Type = contents[start++];
        int valueType = type>> 4 & 3;
        int value_lengthType = type >>4 & 4;
        int valueLength = 0;
        if(value_lengthType == 0){
            valueLength = contents[start++] & 0xFF;
        }else{
            valueLength = (0xFF & contents[start++]) << 8;
            valueLength =  valueLength | (contents[start++] & 0xFF);
        }

        String value = new String(contents, start, valueLength);
        if(keyValueType == 0){
            value = "TOPIC_"+value;
        }else if(keyValueType == 1){
            value = "QUEUE_"+value;
        }else if(keyValueType == 3) {
            value = "PRODUCER"+value;
        }

        if(valueType == 1){
            keyValue.put(key,value);
        }else if(valueType == 0){
            keyValue.put(key,Integer.parseInt(value));
        }else if(valueType == 2){
            keyValue.put(key,Long.parseLong(value));
        }else if(valueType ==3){
            keyValue.put(key,Double.parseDouble(value));
        }

        offset = start+valueLength;

        return true;
    }


    private  int byteToInt2(byte[] b , int start ,int end) {
        int mask=0xff;
        int temp=0;
        int n=0;
        for(int i=start;i<end;i++){
            n<<=8;
            temp=b[i]&mask;
            n|=temp;
        }
        return n;
    }
}
