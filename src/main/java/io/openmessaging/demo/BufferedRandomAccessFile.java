package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Created by H77 on 2017/5/18.
 */
public class BufferedRandomAccessFile extends RandomAccessFile {

    int buffer_size = 1024;
    byte[] buffer = new byte[buffer_size];
    int offset = 0;
    public BufferedRandomAccessFile(String name, String mode) throws FileNotFoundException {
        super(name, mode);
    }
    public BufferedRandomAccessFile(File file, String mode) throws FileNotFoundException {
        super(file, mode);
    }
//    public void write(byte b[]) throws IOException {
//        int length = b.length;
//        if((offset+length) > buffer_size){
//            super.write(buffer,0,offset);
//            offset = 0;
//        }
//        if((offset+length) <= buffer_size) {
//            for (int i = offset; i < offset + length; i++) {
//                buffer[i] = b[i - offset];
//            }
//            offset = offset+length;
//        }else{
//           super.write(b);
//        }
//    }

}
