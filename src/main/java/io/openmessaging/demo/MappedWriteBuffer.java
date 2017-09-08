package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by guojianpeng on 2017/5/23.
 */
public class MappedWriteBuffer {
    private RandomAccessFile randomAccessFile;
    private long bufferSize = 12*1024*1024;
    private MappedByteBuffer contentBuffer = null;
    private FileChannel fc;
    private Long base = 0L;
    private Long currentLength = 0l;

    public MappedWriteBuffer(String fileName) {
        try {
            randomAccessFile = new RandomAccessFile(fileName, "rw");
            fc = randomAccessFile.getChannel();
            contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,0,bufferSize);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public MappedWriteBuffer(RandomAccessFile r){
        this.randomAccessFile =r;
        try {
            fc = randomAccessFile.getChannel();
            contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,0,bufferSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void putContent(byte[] bufferArray, int offset, int length){
        long writeOffset = currentLength - base;
        try {
            if(writeOffset+length>bufferSize){
                contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,currentLength,bufferSize);
                base = currentLength;
                contentBuffer.put(bufferArray,offset,length);
            }
            else{
                contentBuffer.put(bufferArray,offset,length);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        currentLength = currentLength+length;
    }
}
