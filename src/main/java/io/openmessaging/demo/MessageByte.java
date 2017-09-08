package io.openmessaging.demo;

/**
 * Created by H77 on 2017/5/21.
 */
public class MessageByte {
    //加入到WriteThread队列的
    private byte[] buffer = null;
    private int offset = 0;
    public MessageByte(byte[] buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }
    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
