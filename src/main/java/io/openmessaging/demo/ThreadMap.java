package io.openmessaging.demo;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by H77 on 2017/5/19.
 */
public class ThreadMap {
    public static Map<String,WriteThread> files= new ConcurrentHashMap<String,WriteThread>();

//    public static Map<String,WriteBuffer> buffers = new ConcurrentHashMap<>();

    public static Map<String,ReadThread> threads = new ConcurrentHashMap<>();

    public static Map<String,MappedWriteBuffer> mappedWriterMap = new ConcurrentHashMap<>();

    public static MappedWriteBuffer getMappedByteBuffer(String name){
        if (mappedWriterMap.get(name) != null){
            return mappedWriterMap.get(name);
        }
        else{
            try {
                MappedWriteBuffer mappedWriteBuffer = new MappedWriteBuffer(name);
                synchronized (mappedWriterMap) {
                    if (mappedWriterMap.get(name) == null) {
                        mappedWriterMap.put(name, mappedWriteBuffer);
                        return mappedWriteBuffer;
                    } else {
                        return mappedWriterMap.get(name);
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
                return null;
            }
        }
    }


//    public static void setUpWriteThread(String filePrefix) throws Exception{
//        WriteThread file =ThreadMap.files.get(filePrefix);
//        if(file==null){
//            WriteThread writeThread = new WriteThread();
//            RandomAccessFile randomAccessFile = new BufferedRandomAccessFile(filePrefix,"rw");
//            writeThread.setRandomAccessFile(randomAccessFile);
//            if(ThreadMap.files.get(filePrefix)==null) {
//                ThreadMap.files.put(filePrefix, writeThread);
//                Thread thread = new Thread(writeThread);
//                thread.start();
//            }
//        }
//    }

    public static ReadThread setUpReadThread(String key){
        if(threads.get(key) == null){
            int num = ConsumerList.subscirberList.get(key).size();
            ReadThread readThread = new ReadThread(key,num);
            if(threads.get(key) == null){
                threads.put(key,readThread);
                return readThread;
            }else{
                return threads.get(key);
            }
        }else{
            return threads.get(key);
        }
    }


//    public static void setUpBuffer(String key){
//        if(ThreadMap.buffers.get(key)==null){
//            WriteBuffer buffer = new WriteBuffer(2048);
//            buffer.setTopic(key);
//            if(ThreadMap.buffers.get(key) == null){
//                ThreadMap.buffers.put(key, buffer);
//            }else{
//                buffer = null;
//            }
//        }
//    }

}
