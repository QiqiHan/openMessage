package io.openmessaging.demo;

import io.openmessaging.Message;

/**
 * Created by mac on 17/5/22.
 */
public class MessageDTO {

    private Message message;

    private long offset;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
