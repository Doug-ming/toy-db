package com.jd.gaoming.storage.engine.serde;


import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;

class Int64TypeSerdes implements ToydbSerdes<Long> {
    @Override
    public byte[] serialize(Long value, IColumn column) {
        byte[] bytes = new byte[8];

        bytes[0] = (byte) ((value >> 56) & 0xffL);
        bytes[1] = (byte) ((value >> 48) & 0xffL);
        bytes[2] = (byte) ((value >> 40) & 0xffL);
        bytes[3] = (byte) ((value >> 32) & 0xffL);
        bytes[4] = (byte) ((value >> 24) & 0xffL);
        bytes[5] = (byte) ((value >> 16) & 0xffL);
        bytes[6] = (byte) ((value >> 8) & 0xffL);
        bytes[7] = (byte) (value & 0xffL);

        return bytes;
    }

    @Override
    public void serialize(Long value, IColumn column, ByteBuffer buf) {
        buf.put(serialize(value,column));
    }

    @Override
    public Long deserialize(byte[] bytes, IColumn column) {
        long l = 0L;

        l += ((bytes[0] & 0xffL) << 56);
        l += ((bytes[1] & 0xffL) << 48);
        l += ((bytes[2] & 0xffL) << 40);
        l += ((bytes[3] & 0xffL) << 32);
        l += ((bytes[4] & 0xffL) << 24);
        l += ((bytes[5] & 0xffL) << 16);
        l += ((bytes[6] & 0xffL) << 8);
        l += (bytes[7] & 0xffL);

        return l;
    }
}
