package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteBufferTest {
    @Test
    public void test(){
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.put(Constants.MAGIC_BYTES);
        buf.putInt(Constants.MAGIC);

        Assert.assertTrue(buf.position() == 4);
    }
}
