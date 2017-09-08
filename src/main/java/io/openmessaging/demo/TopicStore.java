package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by guojianpeng on 2017/5/22.
 */
public class TopicStore {

    private static List<Message> store1 = new ArrayList<>(1024);
    private static List<Message> store2 = new ArrayList<>(1024);
    private static int inUseNum = 0;

    public static Message getMessage(int offset){
        if(inUseNum == 2) {
            if (offset >= store1.size()) {
                return null;
            } else {
                return store1.get(offset);
            }
        }else if(inUseNum == 1){
            if (offset >= store2.size()) {
                return null;
            } else {
                return store2.get(offset);
            }
        }
        else{
            return null;
        }
    }

    public static void AddMessage(Message message){
        if(inUseNum == 1) {
            store1.add(message);
        }else if(inUseNum == 2){
            store2.add(message);
        }
    }


    public static void changeinUseNum(){
        if(inUseNum <=1){
            inUseNum ++;
            store2.clear();
        }else{
            inUseNum = 1;
            store1.clear();
        }
    }
}
