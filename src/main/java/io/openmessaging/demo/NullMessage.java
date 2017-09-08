package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

/**
 * Created by mac on 17/5/19.
 */
public class NullMessage implements Message {

    @Override
    public KeyValue headers() {
        return null;
    }

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public Message putHeaders(String key, int value) {
        return null;
    }

    @Override
    public Message putHeaders(String key, long value) {
        return null;
    }

    @Override
    public Message putHeaders(String key, double value) {
        return null;
    }

    @Override
    public Message putHeaders(String key, String value) {
        return null;
    }

    @Override
    public Message putProperties(String key, int value) {
        return null;
    }

    @Override
    public Message putProperties(String key, long value) {
        return null;
    }

    @Override
    public Message putProperties(String key, double value) {
        return null;
    }

    @Override
    public Message putProperties(String key, String value) {
        return null;
    }
}
