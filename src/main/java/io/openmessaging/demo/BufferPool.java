package io.openmessaging.demo;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by H77 on 2017/5/23.
 */
public class BufferPool {
    private static List<byte[]> pool;
    private static final int Pool_SIZE = 150;

    public static synchronized byte[] getBuffer(){
        if(pool == null) pool = new ArrayList<>();
        byte[] buffer;
        if(pool.isEmpty()) {
            buffer = new byte[1024*512];
        }else{
            int index = pool.size()-1;
            buffer = pool.get(index);
            pool.remove(index);
        }
        return buffer;
    }
    public static synchronized void returnBuffer(byte[] buffer){
        if(pool.size() <= Pool_SIZE){
            pool.add(buffer);
        }
    }

    public static void clear(){
        pool.clear();
    }
}
