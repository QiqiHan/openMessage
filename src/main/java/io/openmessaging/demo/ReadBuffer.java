package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by guojianpeng on 2017/5/21.
 */
public class ReadBuffer {

    //bufferSize远大于单条信息大小

    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer contentBuffer = null;
    private FileChannel fc;
    private long fileLength;

    //buffer大小控制参数
    //private List<MappedByteBuffer> contentBufferList = new ArrayList<MappedByteBuffer>();
    private long bufferSize;
    private long maxBufferSize = 8*1024*1024;//2G 0x80000000
    private MappedByteBuffer currentContentBuffer = null;//当前buffer
    private int bufferIndex = 0;//当前bufer指针

    //new way



    public ReadBuffer(String fileName) {
        File file = new File(fileName);
        fileLength = file.length();
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            fc = randomAccessFile.getChannel();

            splitBuffer();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public ReadBuffer(RandomAccessFile r){
        this.randomAccessFile =r;
        try {
            fileLength = r.length();
            fc = randomAccessFile.getChannel();
            splitBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public MappedByteBuffer getContentBuffer() {
        return contentBuffer;
    }

    public long getFileLength() {
        return fileLength;
    }

    public boolean isMapped(long fileOffset,int length){
        int neededBufferIndex = (int)(fileOffset/bufferSize);//得到数据所在buffer的index
        int position = (int)(fileOffset%bufferSize);//得到数据在buffer中的位置
        return neededBufferIndex == bufferIndex
                && position+length<=Math.min(bufferSize,getFileLength()-bufferSize*neededBufferIndex);
    }

    public long getContent(byte[] bufferArray, int offset, int length ,long fileOffset){
        //bufferIndex = (int)(fileOffset/bufferSize);//得到数据所在buffer的index
        //new way
        int neededBufferIndex = (int)(fileOffset/bufferSize);//得到数据所在buffer的index
        int position = (int)(fileOffset%bufferSize);//得到数据在buffer中的位置

        //new way
        if (neededBufferIndex != bufferIndex){
            try {
                currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,
                        neededBufferIndex*bufferSize,
                        Math.min(bufferSize,getFileLength()-bufferSize*neededBufferIndex));
                bufferIndex = neededBufferIndex;
//                System.gc();
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        //currentContentBuffer = contentBufferList.get(bufferIndex);
        if(currentContentBuffer.position() != position)
            currentContentBuffer.position(position);//指针移到数据位置

        //判断数据是否在两个buffer中
        if(currentContentBuffer.position()+length <= bufferSize){
            currentContentBuffer.get(bufferArray,offset,length);
        }
        else {
            int partLength = (int) (bufferSize - currentContentBuffer.position());
            currentContentBuffer.get(bufferArray, offset, partLength);

            //currentContentBuffer = contentBufferList.get(++bufferIndex);
            //new way
            neededBufferIndex++;
            try {
                currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,
                        neededBufferIndex*bufferSize,
                        Math.min(bufferSize,getFileLength()-bufferSize*neededBufferIndex));
                bufferIndex = neededBufferIndex;
//                System.gc();
            }catch(Exception e){
                e.printStackTrace();
            }

            currentContentBuffer.get(bufferArray, partLength, length - partLength);
        }
        return currentContentBuffer.position()+bufferSize*bufferIndex;

    }

    //创建buffer列表
    private void splitBuffer(){
        try {
            bufferSize = randomAccessFile.length();
            long length = randomAccessFile.length();
            int bufferNum = 1;
            int position = 0;
            while (bufferSize > maxBufferSize) {
                bufferSize = (int) (bufferSize / 2) + 1;
                bufferNum = bufferNum * 2;
            }

            //new way
            currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0,
                    Math.min(bufferSize,length));
//            System.gc();
            bufferIndex = 0;
            //contentBufferList.add(tempMbb);


            /*for (int i = 0; i < bufferNum; i++) {
                MappedByteBuffer tempMbb = fc.map(FileChannel.MapMode.READ_ONLY, position,
                        Math.min(bufferSize,length-bufferSize*i));
                contentBufferList.add(tempMbb);
                position = (int) (position + bufferSize);
            }*/

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
