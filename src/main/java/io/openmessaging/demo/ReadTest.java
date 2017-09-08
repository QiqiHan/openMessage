package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mac on 17/5/17.
 */
public class ReadTest {

    private static int offset = 0;
    private static int fileoffset = 0;

    public static void main(String[] args) throws Exception{
        RandomAccessFile raf= new RandomAccessFile("/Users/mac/Desktop/test/QUEUE_8","rw" );
        ArrayList<Message> messages = new ArrayList<>(10240);
        while(fileoffset<=raf.length()) {
            DefaultBytesMessage bytesMessage = new DefaultBytesMessage();
            System.out.println(fileoffset);
            System.out.println(raf.length());

            byte[] buf = new byte[4];
            byte[] contents = new byte[2048];
            //读总长度
            raf.read(buf);
            int Alllength = 0;

            Alllength = byteToInt2(buf, 0, 4);

            if(Alllength > 100) {
                System.out.println(Alllength);
            }
            if(Alllength == 0){
                break;
            }

            raf.read(contents, 0, Alllength);

            while (setUpKeyValue(contents, offset, bytesMessage)) ;

            //组装body
            int bodyLength = byteToInt2(contents, offset + 1, offset + 5);
            byte[] body = new byte[bodyLength];
            System.arraycopy(contents, offset + 5, body, 0, bodyLength);
            bytesMessage.setBody(body);
//            System.out.println(new String(body));
            fileoffset = fileoffset+Alllength+4;
            offset = 0;
            messages.add(bytesMessage);
        }

//        System.out.println(messages.size());
    }


    private static boolean setUpKeyValue(byte[] contents,int start,DefaultBytesMessage bytesMessage){

        //判断类型
        int Keytype = contents[start];

        KeyValue keyValue = null;

        if (Keytype == 0){
            keyValue= bytesMessage.headers();
        }else if(Keytype == 1){
            keyValue= bytesMessage.properties();
            if(keyValue == null){
                keyValue = new DefaultKeyValue();
                bytesMessage.setProperties(keyValue);
            }
        }else{
            return false;
        }

        int keyLength = byteToInt2(contents,start+1,start+5);
        String Key = new String(contents,start+5,keyLength);
        int Valuetype = contents[start+5+keyLength];
        int ValueLength = byteToInt2(contents,start+6+keyLength,start+10+keyLength);
        String Value = new String(contents,start+10+keyLength,ValueLength);


        if(Valuetype == 0){
            keyValue.put(Key,Integer.parseInt(Value));
        }else if(Valuetype == 1){
            keyValue.put(Key,Value);
        }else if(Valuetype == 2){
            keyValue.put(Key,Long.parseLong(Value));
        }else if(Valuetype==3){
            keyValue.put(Key,Double.parseDouble(Value));
        }

        offset = start+10+keyLength+ValueLength;

        return true;
    }


    private static int byteToInt2(byte[] b , int start ,int end) {

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
