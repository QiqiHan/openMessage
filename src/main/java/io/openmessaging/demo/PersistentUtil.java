package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mac on 17/5/11.
 */
public class PersistentUtil {

    public static void persistentByteMessage(BytesMessage bytesMessage,BufferedRandomAccessFile file ,byte[] contents) throws IOException {

//        byte[] contents = new byte[2048]; //
        KeyValue keyValue = bytesMessage.headers();
        //从第4位开始写 前4位代表Message长度
        int index = 4;
        if(keyValue != null){
            //0代表head
            index = generateKeyValue(0,keyValue,index,contents);
        }
        KeyValue properties = bytesMessage.properties();
        if(properties != null){
            // 1代表properties
            index = generateKeyValue(1,keyValue,index,contents);
        }
        //下面的代码是处理body 2代表body
        byte[] bodys = bytesMessage.getBody();
        contents[index++] = (byte)2;
        for(int i = 0 ; i <4 ;i++){
            contents[index++] = (byte) (bodys.length>>(24-i*8));
        }
        for(int i = 0 ; i < bodys.length ; i++){
            contents[index++] = bodys[i];
        }

        int messageLength = index-4;
        for(int i = 0 ; i< 4 ; i++){
            contents[i] = (byte)(messageLength>>(24-i*8));
        }
        //将写文件指针移到文件尾。
        long fileLength = file.length();
        file.seek(fileLength);
        file.write(contents,0,index);
    }

    public static int generateKeyValue(int type ,KeyValue keyValue , int index, byte[] contents){
        for(String key : keyValue.keySet()){
            /*   header key  */
            byte[] keyByte = key.getBytes();
            //类型
            contents[index++] =(byte) type;
            //长度
            for(int i = 0 ; i <4 ;i++){
                contents[index++] = (byte) (keyByte.length>>(24-i*8));
            }
            //key内容
            for(int i = 0 ; i < keyByte.length ; i ++){
                contents[index++] = keyByte[i];
            }
            /*   header value  */
            Object value = keyValue.getObject(key);
            byte[] valueByte = null;
            //类型
            if(value instanceof Integer){
                contents[index++] = (byte) 0;
                valueByte = new byte[4];
                for(int i = 0 ; i <4 ;i++){
                    valueByte[i] = (byte) (keyByte.length>>(24-i*8));
                }
            }else if (value instanceof String){
                contents[index++] = (byte) 1;
                valueByte = ((String)value).getBytes();
            }else if(value instanceof Long){
                contents[index++] = (byte) 2;
                ByteBuffer buffer = ByteBuffer.allocate(8);
                valueByte = buffer.putLong((Long)value).array();
            }else if(value instanceof Double){
                contents[index++] = (byte) 3;
                ByteBuffer buffer = ByteBuffer.allocate(8);
                valueByte = buffer.putDouble((Double)value).array();
            }
            //长度
            for(int i = 0 ; i< 4 ; i++){
                contents[index++] = (byte)(valueByte.length>>(24-i*8));
            }
            //value内容
            for (int i = 0 ; i <valueByte.length ; i++){
                contents[index++] = valueByte[i];
            }
        }
        return index;
    }

    public static void readByteMessage(){
        BytesMessage message = new DefaultBytesMessage(new byte[1]) ;

    }

    public static void writeByteMessage(BytesMessage bytesMessage,BufferedRandomAccessFile file) throws IOException {
        StringBuilder data = new StringBuilder("{");
//        StringBuilder header = generateData(bytesMessage.headers(),"header");
        data.append("{");
        data.append(new String(bytesMessage.getBody()));
        data.append("}");

//        int headersize = header.toString().getBytes().length;
//        int bodysize = bytesMessage.getBody().length;
//        int propsize = properties.toString().getBytes().length;

        long fileLength = file.length();
//         将写文件指针移到文件尾。
        file.seek(fileLength);
        file.write(data.toString().getBytes());
    }

    public static void persistentData(StringBuilder builder,RandomAccessFile file) throws IOException {

//        int headersize = header.toString().getBytes().length;
//        int bodysize = bytesMessage.getBody().length;
//        int propsize = properties.toString().getBytes().length;

        long fileLength = file.length();
//         将写文件指针移到文件尾。
        file.seek(fileLength);
        file.write(builder.toString().getBytes());
    }

    public static StringBuilder buildData(BytesMessage bytesMessage, StringBuilder builder) throws IOException {
        StringBuilder data = builder;
        data = generateData(bytesMessage.headers(),"header" ,data);
        data = generateData(bytesMessage.properties(),"properties" ,data);
        data.append("body:{");
        data.append(new String(bytesMessage.getBody()));
        data.append("}");
        return data;
    }

    private static StringBuilder generateData(KeyValue keyValue,String tag ,StringBuilder sb){
         sb.append(tag);
        if(keyValue == null ){
            return sb.append(":{}");
        }
        sb.append(":");
        for(String key : keyValue.keySet()){
            sb.append("{");
            sb.append(key);
            sb.append(",");
            sb.append(keyValue.getString(key));
            sb.append("}");
        }
        return sb;
    }

    public static void main(String[] args){

        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", "/Users/mac/Desktop");

        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        Producer producer = new DefaultProducer(properties);

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE2"; //实际测试时，queue数目与消费线程数目相同
        List<Message> messagesForTopic1 = new ArrayList<>(1024);
        List<Message> messagesForTopic2 = new ArrayList<>(1024);
        List<Message> messagesForQueue1 = new ArrayList<>(1024);
        List<Message> messagesForQueue2 = new ArrayList<>(1024);
        for (int i = 0; i < 2; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1,  (topic1 + i).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        for (int i = 0; i < 2; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
        long end = System.currentTimeMillis();

        long T1 = end - start;

    }
}
