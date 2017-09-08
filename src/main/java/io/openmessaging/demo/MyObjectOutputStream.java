package io.openmessaging.demo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Created by mac on 17/5/19.
 */
public class MyObjectOutputStream extends ObjectOutputStream {
    public MyObjectOutputStream() throws IOException
    {
        super();
    }

    public MyObjectOutputStream(OutputStream out) throws IOException
    {
        super(out);
    }

    @Override
    protected void writeStreamHeader() throws IOException
    {
        return;
    }
}
