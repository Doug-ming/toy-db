package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;

class Int32TypeSerdes implements ToydbSerdes<Integer> {
    @Override
    public byte[] serialize(Integer value, IColumn column) {
        byte[] bytes = new byte[4];

        bytes[0] = (byte) ((value >> 24) & 0xff);
        bytes[1] = (byte) ((value >> 16) & 0xff);
        bytes[2] = (byte) ((value >> 8) & 0xff);
        bytes[3] = (byte) (value & 0xff);

        return bytes;
    }

    @Override
    public void serialize(Integer value, IColumn column, ByteBuffer buf) {
        buf.put(serialize(value,column));
    }

    @Override
    public Integer deserialize(byte[] bytes, IColumn column) {
        int i = 0;

        i += ((bytes[0] & 0xff) << 24);
        i += ((bytes[1] & 0xff) << 16);
        i += ((bytes[2] & 0xff) << 8);
        i += bytes[3] & 0xff;

        return i;
    }
}
