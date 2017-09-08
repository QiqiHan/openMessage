package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;


import java.nio.ByteBuffer;


/**
 * Created by mac on 17/5/11.
 */
public class EncodeUtil {

    public static int encodeByteMessage(BytesMessage bytesMessage , byte[] contents)  {

         KeyValue keyValue = bytesMessage.headers();
         //从第4位开始写 前4位代表Message长度
        //实际情况中 length 用2个字节就能表示
         int index = 2;
         if (keyValue != null) {
             //0代表head
             index = generateKeyValue(0, keyValue, index, contents);
         }
         KeyValue properties = bytesMessage.properties();
         if (properties != null) {
             // 1代表properties
             index = generateKeyValue(1, properties, index, contents);
         }
         //对于body type 的低4位中 低2位表示
         //下面的代码是处理body 2代表body
         byte[] bodys = bytesMessage.getBody();
         int bodyOffset = index;
         contents[index++] = (byte) 2;
        //判断body length的长度
         if(bodys.length <= 255){
             contents[index++] = (byte) bodys.length;
         }else{
             contents[bodyOffset] = (byte) (contents[bodyOffset] | 4);
             for (int i = 0; i < 2; i++) {
                 contents[index++] = (byte) (bodys.length >> (8 - i * 8));
             }
         }
         for (int i = 0; i < bodys.length; i++) {
             contents[index++] = bodys[i];
         }

         int messageLength = index - 2;
         for (int i = 0; i < 2; i++) {
             contents[i] = (byte) (messageLength >> (8 - i * 8));
         }
         return index;
    }

    public static int generateKeyValue(int type , KeyValue keyValue , int index, byte[] contents){
        for(String key : keyValue.keySet()){
            int Type_Offset = index++;
            contents[Type_Offset] =(byte) type;
            //似乎value也可以压缩的
            if (key.equals("Topic")) {
                contents[index++] = (byte)0;
            }else if(key.equals("Queue")){
                 contents[index++] = (byte)1;
            }else if(key.equals("MessageId")){
                 contents[index++] = (byte)2;
            }else if(key.equals("PRO_OFFSET")){
                 contents[index++] = (byte)3;
            }else {
               /*   header key  */
                byte[] keyByte = key.getBytes();
                contents[index++] = (byte)4;
                //类型
                //key    低4位 低两位表示类型 第三位表示lengthType
                //value  高4位 低两位表示类型 第三位表示lengthType
                if (keyByte.length <= 255) {
                    //这种情况 keyType_Offset 第3位就是0
                    contents[index++] = (byte) keyByte.length;
                } else {
                    //这种情况 keyType_Offset 第3位就是1
                    contents[Type_Offset] = (byte) (contents[Type_Offset] | 4);
                    for (int i = 0; i < 2; i++) {
                        contents[index++] = (byte) (keyByte.length >> (8 - i * 8));
                    }
                }
                //key内容
                for (int i = 0; i < keyByte.length; i++) {
                    contents[index++] = keyByte[i];
                }
            }

            /*   header value  */
            Object value = ((DefaultKeyValue)keyValue).getObject(key);
            byte[] valueByte = null;
            //类型 最低两位表示类型
            int initOffset = 0;
            if (value instanceof String){
                //010000
                contents[Type_Offset] = (byte) (contents[Type_Offset] | 16);
                if (key.equals("Topic")) {
                    initOffset = 6;
//                    valueContent = keyValue.getString("Topic").substring(6);
                }else if(key.equals("Queue")){
                    initOffset = 6;
//                    valueContent = keyValue.getString("Queue").substring(6);
                }else if(key.equals("PRO_OFFSET")){
                    initOffset = 8;
//                    valueContent = keyValue.getString("PRO_OFFSET").substring(8);
                }
                valueByte = ((String)value).getBytes();
            }else if(value instanceof Integer){
                //000000
                ByteBuffer buffer = ByteBuffer.allocate(4);
                valueByte = buffer.putInt((Integer)value).array();
            }else if(value instanceof Long){
                //100000
                contents[Type_Offset] = (byte) (contents[Type_Offset] | 32);
                ByteBuffer buffer = ByteBuffer.allocate(8);
                valueByte = buffer.putLong((Long)value).array();
            }else if(value instanceof Double){
                //110000
                contents[Type_Offset] = (byte) (contents[Type_Offset] | 48);
                ByteBuffer buffer = ByteBuffer.allocate(8);
                valueByte = buffer.putDouble((Double)value).array();
            }

            int valueLength = valueByte.length - initOffset;
            if(valueByte.length <= 255) {
                //000 0000
                contents[index++] = (byte) valueLength;
            }else {
                //100 0000
                contents[Type_Offset] = (byte) (contents[Type_Offset] | 64);
                for(int i = 0 ; i <2 ; i++){
                    contents[index++] = (byte) (valueLength>>(8-i*8));
                }
            }
            //value内容
            for (int i = initOffset ; i <valueByte.length ; i++){
                contents[index++] = valueByte[i];
            }

        }
        return index;
    }

}
